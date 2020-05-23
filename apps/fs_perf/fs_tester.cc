/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2020 ScyllaDB
 */

#include "fs_tester.hh"

#include <algorithm>
#include <boost/range/irange.hpp>
#include <cassert>
#include <cstdint>
#include <random>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/fs/bitwise.hh>

using namespace seastar;
using namespace seastar::fs;

temporary_buffer<uint8_t> fs_tester::share_base_buffer(size_t size) {
    auto tmp = _base_buffer.share();
    tmp.trim_front(tmp.size() - size);
    return tmp;
}

temporary_buffer<uint8_t> fs_tester::gen_small_buffer() {
    return share_base_buffer(gen_small_op_size());
}

temporary_buffer<uint8_t> fs_tester::gen_big_buffer() {
    return share_base_buffer(gen_big_op_size());
}

temporary_buffer<uint8_t> fs_tester::allocate_read_buffer(size_t size) {
    return _rcfg.aligned ? temporary_buffer<uint8_t>::aligned(_rcfg.alignment, size) :
            temporary_buffer<uint8_t>(size);
}

size_t fs_tester::gen_small_op_size() {
    return _gen_small_op_size();
}

size_t fs_tester::gen_big_op_size() {
    return _gen_big_op_size();
}

size_t fs_tester::gen_op_pos(size_t min, size_t max) {
    if (_rcfg.aligned) {
        auto aligned_min = round_up_to_multiple_of_power_of_2(min, _rcfg.alignment);
        auto aligned_max = round_down_to_multiple_of_power_of_2(max, _rcfg.alignment);
        auto dist = std::uniform_int_distribution<size_t>(aligned_min, aligned_max);
        return round_down_to_multiple_of_power_of_2(dist(_random_engine), _rcfg.alignment);
    } else {
        auto dist = std::uniform_int_distribution<size_t>(min, max);
        return dist(_random_engine);
    }
}

future<> fs_tester::do_write() {
    file_info* file;
    temporary_buffer<uint8_t> buff;
    if (_prob_dist(_random_engine) < _rcfg.small_prob) {
        file = &_small_files[_small_files_dist(_random_engine)];
        buff = gen_small_buffer();
    } else {
        file = &_big_files[_big_files_dist(_random_engine)];
        buff = gen_big_buffer();
    }

    size_t write_pos;
    if (_rcfg.seq_writes) {
        write_pos = file->_size;
        file->_size += buff.size();
    } else {
        write_pos = gen_op_pos(0, file->_size);
        file->_size = std::max(file->_size, write_pos + buff.size());
    }
    total_write_len += buff.size();

    return file->_file.dma_write(write_pos, buff.get(), buff.size()).then([buff = std::move(buff)](size_t write_len) {
        assert(write_len == buff.size());
    });
}

future<> fs_tester::do_read() { // TODO: dont read from holes
    file file;
    bool small_read = _prob_dist(_random_engine) < _rcfg.small_prob;
    if (small_read) {
        file = _small_files[_small_files_dist(_random_engine)]._file;
    } else {
        file = _big_files[_big_files_dist(_random_engine)]._file;
    }
    return file.size().then([this, file = std::move(file), small_read](size_t file_size) mutable {
        size_t size = std::min(file_size, small_read ? gen_small_op_size() : gen_big_op_size());
        total_read_len += size;
        size_t read_pos = gen_op_pos(0, file_size - size);
        auto buff = allocate_read_buffer(size);
        return file.dma_read(read_pos, buff.get_write(), buff.size()).then([buff = std::move(buff)](size_t read_len) {
            assert(read_len == buff.size());
        });
    });
}

future<> fs_tester::run() {
    // TODO: maybe add parallelism to config
    return do_with(semaphore(64), gate(), (size_t)0, [this] (semaphore& write_parallelism, gate& gate, size_t& iter) {
        return do_until([this, &iter] {
            return (_rcfg.written_data_limit && total_write_len >= *_rcfg.written_data_limit) ||
                    (_rcfg.read_data_limit && total_read_len >= *_rcfg.read_data_limit) ||
                    (_rcfg.op_nb_limit && iter >= _rcfg.op_nb_limit);
        }, [this, &write_parallelism, &gate, &iter] {
            return get_units(write_parallelism, 1).then([this, &gate] (auto units) {
                gate.enter();
                future<> op_future = _prob_dist(_random_engine) < _rcfg.write_prob ? do_write() : do_read();
                auto tmp = op_future.finally([&gate, units = std::move(units)] {
                    gate.leave();
                });
            }).then([&iter] {
                iter++;
            });
        }).finally([&gate] {
            return gate.close();
        });
    }).then([this] {
        return post_test_callback();
    });
}

void fs_tester::setup_generators() {
    auto create_op_size_generator =
            [this, aligned = _rcfg.aligned, alignment = _rcfg.alignment](size_t min, size_t max) ->
            std::function<size_t()> {
        if (aligned) {
            auto aligned_min = round_up_to_multiple_of_power_of_2(min, alignment);
            auto aligned_max = round_down_to_multiple_of_power_of_2(max, alignment);
            auto dist = std::uniform_int_distribution<size_t>(aligned_min, aligned_max);
            return [this, alignment, dist = std::move(dist)]() mutable {
                auto ret = round_down_to_multiple_of_power_of_2(dist(_random_engine), alignment);
                return ret;
            };
        } else {
            auto dist = std::uniform_int_distribution<size_t>(min, max);
            return [this, dist = std::move(dist)]() mutable {
                return dist(_random_engine);
            };
        }
    };

    _gen_small_op_size = create_op_size_generator(_rcfg.small_op_size_range.first, _rcfg.small_op_size_range.second);
    _gen_big_op_size = create_op_size_generator(_rcfg.big_op_size_range.first, _rcfg.big_op_size_range.second);

    _base_buffer = [this] {
        temporary_buffer<uint8_t> ret;
        size_t max_size = std::max(_rcfg.small_op_size_range.second, _rcfg.big_op_size_range.second);
        ret = _rcfg.aligned ? temporary_buffer<uint8_t>::aligned(_rcfg.alignment,
                round_down_to_multiple_of_power_of_2(max_size, _rcfg.alignment)) :
                temporary_buffer<uint8_t>(max_size);
        auto dist = std::uniform_int_distribution<uint8_t>();
        std::generate_n(ret.get_write(), ret.size(), [&] { return dist(_random_engine); });
        return ret;
    }();
}

future<> fs_tester::init() {
    return async([&] {
        setup_generators();
        setup_fs_state();
    });
}

future<> fs_tester::stop() {
    return async([&] {
        for (auto& file : _small_files) {
            file._file.close().get();
        }
        for (auto& file : _big_files) {
            file._file.close().get();
        }
    });
}
