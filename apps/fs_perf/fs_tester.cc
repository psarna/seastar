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
#include "seastar/util/log.hh"

#include <algorithm>
#include <boost/range/irange.hpp>
#include <cassert>
#include <cstdint>
#include <exception>
#include <optional>
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

namespace {
logger mlogger("fs_perf");
} // namespace

fs_tester::fs_tester(run_config rconf)
    : _recfg {
        .op_nb_limit = std::move(rconf.op_nb_limit),
        .written_data_limit = std::move(rconf.written_data_limit),
        .read_data_limit = std::move(rconf.read_data_limit),
        .parallelism = rconf.parallelism,
        .write_prob = rconf.write_prob,
    }
    , _dgcfg {
        .small_files_nb = rconf.small_files_nb,
        .big_files_nb = rconf.big_files_nb,
        .big_op_size_range = rconf.big_op_size_range,
        .small_op_size_range = rconf.small_op_size_range,
        .small_prob = rconf.small_prob,
        .small_write_prob = rconf.small_write_prob,
        .alignment = rconf.alignment,
        .aligned_ops = rconf.aligned_ops,
        .seq_writes = rconf.seq_writes,
    }
    , _prob_dist(0, 1)
    , _small_files_dist(0, rconf.small_files_nb == 0 ? 0 : rconf.small_files_nb - 1)
    , _big_files_dist(0, rconf.big_files_nb == 0 ? 0 : rconf.big_files_nb - 1) {}

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
    return _dgcfg.aligned_ops ? temporary_buffer<uint8_t>::aligned(_dgcfg.alignment, size) :
            temporary_buffer<uint8_t>(size);
}

size_t fs_tester::gen_small_op_size() {
    return _gen_small_op_size();
}

size_t fs_tester::gen_big_op_size() {
    return _gen_big_op_size();
}

size_t fs_tester::gen_op_pos(size_t min, size_t max) {
    if (_dgcfg.aligned_ops) {
        auto aligned_min = round_up_to_multiple_of_power_of_2(min, _dgcfg.alignment);
        auto aligned_max = round_down_to_multiple_of_power_of_2(max, _dgcfg.alignment);
        auto dist = std::uniform_int_distribution<size_t>(aligned_min, aligned_max);
        return round_down_to_multiple_of_power_of_2(dist(_random_engine), _dgcfg.alignment);
    } else {
        auto dist = std::uniform_int_distribution<size_t>(min, max);
        return dist(_random_engine);
    }
}

future<> fs_tester::do_truncate() {
    file_info* file;
    size_t truncate_size;
    if (_prob_dist(_random_engine) < _dgcfg.small_prob) {
        file = &_small_files[_small_files_dist(_random_engine)];
        truncate_size = gen_small_op_size();
    } else {
        file = &_big_files[_big_files_dist(_random_engine)];
        truncate_size = gen_big_op_size();
    }
    truncate_size = truncate_size > file->_size ? 0 : file->_size - truncate_size;
    total_files_size -= file->_size - truncate_size;
    file->_size = truncate_size;

    return file->_file.truncate(truncate_size);
}

future<> fs_tester::do_write(size_t& total_write_len) {
    file_info* file;
    temporary_buffer<uint8_t> buff;
    if (_prob_dist(_random_engine) < _dgcfg.small_write_prob) {
        file = &_small_files[_small_files_dist(_random_engine)];
        buff = gen_small_buffer();
    } else {
        file = &_big_files[_big_files_dist(_random_engine)];
        buff = gen_big_buffer();
    }

    size_t write_pos;
    if (_dgcfg.seq_writes) {
        write_pos = file->_size;
        file->_size += buff.size();
        total_files_size += buff.size();
    } else {
        write_pos = gen_op_pos(0, file->_size);
        size_t write_end = write_pos + buff.size();
        if (write_end > file->_size) {
            total_files_size += write_end - file->_size;
            file->_size = write_end;
        }
    }

    total_write_len += buff.size();

    return file->_file.dma_write(write_pos, buff.get(), buff.size()).then([buff = std::move(buff)](size_t write_len) {
        assert(write_len == buff.size());
    });
}

future<> fs_tester::do_read(size_t& total_read_len) { // TODO: dont read from holes
    file_info* file;
    size_t read_size;
    if (_prob_dist(_random_engine) < _dgcfg.small_prob) {
        file = &_small_files[_small_files_dist(_random_engine)];
        read_size = gen_small_op_size();
    } else {
        file = &_big_files[_big_files_dist(_random_engine)];
        read_size = gen_big_op_size();
    }
    read_size = std::min(file->_size, read_size);

    total_read_len += read_size;
    size_t read_pos = gen_op_pos(0, file->_size - read_size);
    auto buff = allocate_read_buffer(read_size);
    return file->_file.dma_read(read_pos, buff.get_write(), buff.size()).then([buff = std::move(buff)](size_t read_len) {
    });
}

future<> fs_tester::run_execution(run_execution_config& recfg) {
    return async([this, &recfg] {
        semaphore write_parallelism(recfg.parallelism);
        gate gate;
        size_t iter = 0;
        size_t total_write_len = 0;
        size_t total_read_len = 0;
        bool truncate_mode = false;
        std::optional<std::exception_ptr> exception_occurred;
        auto stop_run = [&] {
            return (recfg.written_data_limit && total_write_len >= *recfg.written_data_limit) ||
                    (recfg.read_data_limit && total_read_len >= *recfg.read_data_limit) ||
                    (recfg.op_nb_limit && iter >= recfg.op_nb_limit);
        };
        while (!stop_run()) {
            auto units = get_units(write_parallelism, 1).get0();
            if (exception_occurred) {
                break;
            }
            gate.enter();
            future<> op_future = now();
            if (3 * total_files_size >= 2 * filesystem_size()) {
                truncate_mode = true;
            } else if (3 * total_files_size < filesystem_size()) {
                truncate_mode = false;
            }

            if (truncate_mode) {
                op_future = do_truncate();
            } else {
                if (_prob_dist(_random_engine) < 0.2) {
                    op_future = do_truncate();
                } else {
                    if (_prob_dist(_random_engine) < recfg.write_prob) {
                        op_future = do_write(total_write_len);
                    } else {
                        op_future = do_read(total_read_len);
                    }
                }
            }
            auto tmp = op_future.handle_exception([&exception_occurred](std::exception_ptr e) {
                mlogger.info("Exception occurred during run: {}", e);
                exception_occurred = std::move(e);
            }).finally([&gate, units = std::move(units)] {
                gate.leave();
            });
            iter++;
        }
        gate.close().get();

        if (exception_occurred) {
            std::rethrow_exception(*exception_occurred);
        }

        post_test_callback().get();
    });
}

future<> fs_tester::run() {
    return run_execution(_recfg);
}

void fs_tester::setup_generators() {
    auto create_op_size_generator =
            [this, aligned = _dgcfg.aligned_ops, alignment = _dgcfg.alignment](size_t min, size_t max) ->
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

    _gen_small_op_size = create_op_size_generator(_dgcfg.small_op_size_range.first, _dgcfg.small_op_size_range.second);
    _gen_big_op_size = create_op_size_generator(_dgcfg.big_op_size_range.first, _dgcfg.big_op_size_range.second);

    _base_buffer = [this] {
        temporary_buffer<uint8_t> ret;
        size_t max_size = std::max(_dgcfg.small_op_size_range.second, _dgcfg.big_op_size_range.second);
        ret = _dgcfg.aligned_ops ? temporary_buffer<uint8_t>::aligned(_dgcfg.alignment,
                round_down_to_multiple_of_power_of_2(max_size, _dgcfg.alignment)) :
                temporary_buffer<uint8_t>(max_size);
        auto dist = std::uniform_int_distribution<uint8_t>();
        std::generate_n(ret.get_write(), ret.size(), [&] {
            thread::maybe_yield();
            return dist(_random_engine);
        });
        return ret;
    }();
}

void fs_tester::setup_fs_state() {
    auto init_files = [&](std::vector<file_info>& files, auto gen_buffer) {
        for (auto& f : files) {
            for (size_t i = 0; i < 3; ++i) {
                auto buff = gen_buffer();
                f._file.dma_write(f._size, buff.get(), buff.size()).then([&](size_t write_len) {
                    assert(write_len == buff.size());
                }).get();
                f._size += buff.size();
            }
            total_files_size += f._size;
        }
    };
    // Generate some initial data for each file
    init_files(_small_files, [this] { return gen_small_buffer(); });
    init_files(_big_files, [this] { return gen_big_buffer(); });

    run_execution_config recfg {
        .op_nb_limit = std::nullopt,
        .written_data_limit = 2 * filesystem_size(),
        .read_data_limit = std::nullopt,
        .parallelism = 64,
        .write_prob = 1,
    };

    // Write around 2 * filesystem-size so we won't measure time on (nearly) brand new filesystem.
    run_execution(recfg).get();
}

future<> fs_tester::init() {
    return async([&] {
        setup_generators();
        create_files();
        assert(_small_files.size() == _dgcfg.small_files_nb);
        assert(_big_files.size() == _dgcfg.big_files_nb);
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
