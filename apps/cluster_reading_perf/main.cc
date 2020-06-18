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

#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/units.hh>
#include <seastar/core/thread.hh>
#include <seastar/fs/block_device.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/conversions.hh>
#include <random>
#include <stdint.h>
#include <iostream>
#include <fstream>

using namespace seastar;
using namespace fs;

constexpr uint64_t alignment = 4096;

static future<temporary_buffer<char>> allocate_random_aligned_buffer(size_t size) {
    return do_with(temporary_buffer<char>::aligned(alignment, size),
            std::default_random_engine(), [size](auto& buffer, auto& random_engine) {
        return do_for_each(buffer.get_write(), buffer.get_write() + size, [&](char& c) {
            std::uniform_int_distribution<> character(0, sizeof(char) * 8 - 1);
            c = character(random_engine);
        }).then([&buffer] {
            return std::move(buffer);
        });
    });
}

template <typename Func>
void measure(const std::string& str, size_t repeats, Func&& func) {
    auto st = std::chrono::system_clock::now();
    func();
    auto ed = std::chrono::system_clock::now();
    std::cerr << str << "time: " << std::chrono::duration_cast<std::chrono::milliseconds>(ed - st).count() / repeats << "ms" << std::endl;
}

struct drange {
    size_t beg;
    size_t size;
};

future<> perform_partial_reads_parallel(block_device& dev, size_t min_beg, std::vector<drange>& ranges, temporary_buffer<char>& buffer) {
    return parallel_for_each(ranges, [&dev, &buffer, min_beg](drange& range) {
        return dev.read(min_beg + range.beg, buffer.get_write() + range.beg, range.size).then(
                [expected_read_len = range.size](size_t read_len) {
            assert(read_len == expected_read_len);
        });
    });
}

future<> perform_partial_reads_sequential(block_device& dev, size_t min_beg, std::vector<drange>& ranges, temporary_buffer<char>& buffer) {
    return do_for_each(ranges, [&dev, &buffer, min_beg](drange& range) {
        return dev.read(min_beg + range.beg, buffer.get_write() + range.beg, range.size).then(
                [expected_read_len = range.size](size_t read_len) {
            assert(read_len == expected_read_len);
        });
    });
}

std::vector<size_t> gen_sizes(size_t size, size_t parts, bool allow_zeros) {
    std::default_random_engine random_engine;
    auto generate_random_value = [&](size_t min, size_t max) {
        return std::uniform_int_distribution<size_t>(min, max)(random_engine);
    };
    std::vector<size_t> begs;
    if (allow_zeros) {
        std::multiset<size_t> begs_set;
        begs_set.emplace(0);
        begs_set.emplace(size);
        while (begs_set.size() != parts + 1) {
            begs_set.emplace(generate_random_value(0, size) / alignment * alignment);
        }
        for (size_t i : begs_set) {
            begs.emplace_back(i);
        }
    } else {
        std::set<size_t> begs_set;
        begs_set.emplace(0);
        begs_set.emplace(size);
        while (begs_set.size() != parts + 1) {
            begs_set.emplace(generate_random_value(0, size) / alignment * alignment);
        }
        for (size_t i : begs_set) {
            begs.emplace_back(i);
        }
    }
    std::vector<size_t> sizes;
    for (size_t i = 1; i < begs.size(); ++i) {
        sizes.emplace_back(begs[i] - begs[i - 1]);
    }
    return sizes;
}

std::vector<drange> gen_ranges(size_t max_size, size_t read_size, size_t parts) {
    auto read_sizes = gen_sizes(read_size, parts, false);
    auto hole_sizes = gen_sizes(max_size - read_size, parts + 1, true);

    std::vector<drange> vec;
    size_t beg = hole_sizes[0];
    for (size_t i = 0; i < read_sizes.size(); ++i) {
        vec.emplace_back(drange {beg, read_sizes[i]});
        beg += read_sizes[i];
        beg += hole_sizes[i + 1];
    }
    return vec;
}

void create_file(const std::string& name) {
    std::fstream stream;
    stream.open(name, std::ios::out);
    stream.close();
}

int main(int argc, char** argv) {
    app_template app;
    app.add_options()
            ("cluster_size", boost::program_options::value<std::string>()->default_value("4M"))
            ("chunks_size", boost::program_options::value<std::string>()->default_value("400k"),
                    "Accumulative size of all active chunks")
            ("chunks_number", boost::program_options::value<size_t>()->default_value(20),
                    "Number of active chunks")
            ("repeats", boost::program_options::value<size_t>()->default_value(100));
    return app.run(argc, argv, [&app] {
        return async([&] {
            auto& args = app.configuration();
            const std::string file_name = "block_device_test_file";
            create_file(file_name);
            block_device dev = open_block_device("block_device_test_file").get0();
            auto close_dev = defer([&dev]() mutable { dev.close().get0(); });

            size_t cluster_size = parse_memory_size(args["cluster_size"].as<std::string>());
            size_t chunk_sum_size = parse_memory_size(args["chunks_size"].as<std::string>());
            size_t chunks_nb = args["chunks_number"].as<size_t>();
            size_t repeats = args["repeats"].as<size_t>();

            temporary_buffer<char> buffer;
            buffer = allocate_random_aligned_buffer(cluster_size).get0();
            for (size_t i = 0; i < repeats; ++i) {
                assert(dev.write(i * cluster_size, buffer.get(), cluster_size).get0() == cluster_size);
            }

            measure("whole cluster: ", repeats, [&] {
            for (size_t i = 0; i < repeats; ++i) {
                assert(dev.read(i * cluster_size, buffer.get_write(), cluster_size).get0() == cluster_size);
            }
            });

            std::vector<std::vector<drange>> ranges(repeats, std::vector<drange>());
            for (size_t i = 0; i < repeats; ++i) {
                ranges[i] = gen_ranges(cluster_size, chunk_sum_size, chunks_nb);
            }

            measure("chunks (do_for_each): ", repeats, [&] {
            for (size_t i = 0; i < repeats; ++i) {
                perform_partial_reads_sequential(dev, i * cluster_size, ranges[i], buffer).get();
            }
            });

            measure("chunks (parallel_for_each): ", repeats, [&] {
            for (size_t i = 0; i < repeats; ++i) {
                perform_partial_reads_parallel(dev, i * cluster_size, ranges[i], buffer).get();
            }
            });

            std::vector<drange> new_ranges;
            for (size_t beg = 0; beg < cluster_size; beg += 16 * alignment) {
                new_ranges.emplace_back(drange {beg, 16 * alignment});
            }
            measure("whole cluster (do_for_each): ", repeats, [&] {
            for (size_t i = 0; i < repeats; ++i) {
                perform_partial_reads_sequential(dev, i * cluster_size, new_ranges, buffer).get();
            }
            });

        });
    });
}
