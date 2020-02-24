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
 * Copyright (C) 2019 ScyllaDB
 */

#include "seastar/core/do_with.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/temporary_buffer.hh"

#include <boost/range/irange.hpp>
#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/units.hh>
#include <seastar/fs/block_device.hh>
#include <seastar/fs/temporary_file.hh>
#include <seastar/testing/test_runner.hh>
#include <unistd.h>

using namespace seastar;
using namespace seastar::fs;

constexpr off_t min_device_size = 16*MB;
constexpr size_t alignment = 4*KB;

static future<temporary_buffer<char>> allocate_random_aligned_buffer(size_t size) {
    return do_with(temporary_buffer<char>::aligned(alignment, size), std::default_random_engine(testing::local_random_engine()), [size](auto& buffer, auto& random_engine) {
        return do_for_each(buffer.get_write(), buffer.get_write() + size, [&](char& c) {
            std::uniform_int_distribution<> character(0, sizeof(char) * 8 - 1);
            c = character(random_engine);
        }).then([&buffer] {
            return std::move(buffer);
        });
    });
}

static future<> test_basic_read_write(const std::string& device_path) {
    return async([&] {
        block_device dev = open_block_device(device_path).get0();
        constexpr size_t buff_size = 16*KB;
        auto buffer = allocate_random_aligned_buffer(buff_size).get0();
        auto check_buffer = allocate_random_aligned_buffer(buff_size).get0();

        // Write and read
        assert(dev.write(0, buffer.get(), buff_size).get0() == buff_size);
        assert(dev.read(0, check_buffer.get_write(), buff_size).get0() == buff_size);
        assert(std::memcmp(buffer.get(), check_buffer.get(), buff_size) == 0);

        // Data have to remain after closing
        dev.close().get0();
        dev = open_block_device(device_path).get0();
        check_buffer = allocate_random_aligned_buffer(buff_size).get0(); // Make sure the buffer is written
        assert(dev.read(0, check_buffer.get_write(), buff_size).get0() == buff_size);
        assert(std::memcmp(buffer.get(), check_buffer.get(), buff_size) == 0);

        dev.close().get0();
    });
}

static future<> test_parallel_read_write(const std::string& device_path) {
    return async([&] {
        block_device dev = open_block_device(device_path).get0();
        constexpr size_t buff_size = 16*MB;
        auto buffer = allocate_random_aligned_buffer(buff_size).get0();

        // Write
        static_assert(buff_size % alignment == 0);
        parallel_for_each(boost::irange<off_t>(0, buff_size / alignment), [&](off_t block_no) {
            off_t offset = block_no * alignment;
            return dev.write(offset, buffer.get() + offset, alignment).then([](size_t written) {
                assert(written == alignment);
            });
        }).get0();

        // Read
        static_assert(buff_size % alignment == 0);
        parallel_for_each(boost::irange<off_t>(0, buff_size / alignment), [&](off_t block_no) {
            return async([&dev, &buffer, block_no] {
                off_t offset = block_no * alignment;
                auto check_buffer = allocate_random_aligned_buffer(alignment).get0();
                assert(dev.read(offset, check_buffer.get_write(), alignment).get0() == alignment);
                assert(std::memcmp(buffer.get() + offset, check_buffer.get(), alignment) == 0);
            });
        }).get0();

        dev.close().get0();
    });
}

static future<> test_simultaneous_parallel_read_and_write(const std::string& device_path) {
    return async([&] {
        block_device dev = open_block_device(device_path).get0();
        constexpr size_t buff_size = 16*MB;
        auto buffer = allocate_random_aligned_buffer(buff_size).get0();
        assert(dev.write(0, buffer.get(), buff_size).get0() == buff_size);

        static_assert(buff_size % alignment == 0);
        size_t blocks_num = buff_size / alignment;
        enum Kind { WRITE, READ };
        std::vector<Kind> block_kind(blocks_num);
        std::default_random_engine random_engine(testing::local_random_engine());
        std::uniform_int_distribution<> choose_write(0, 1);
        for (Kind& kind : block_kind) {
            kind = (choose_write(random_engine) ? WRITE : READ);
        }

        // Perform simultaneous reads and writes
        auto new_buffer = allocate_random_aligned_buffer(buff_size).get0();
        auto write_fut = parallel_for_each(boost::irange<off_t>(0, blocks_num), [&](off_t block_no) {
            if (block_kind[block_no] != WRITE) {
                return now();
            }

            off_t offset = block_no * alignment;
            return dev.write(offset, new_buffer.get() + offset, alignment).then([](size_t written) {
                assert(written == alignment);
            });
        });
        auto read_fut = parallel_for_each(boost::irange<off_t>(0, blocks_num), [&](off_t block_no) {
            if (block_kind[block_no] != READ) {
                return now();
            }

            return async([&dev, &buffer, block_no] {
                off_t offset = block_no * alignment;
                auto check_buffer = allocate_random_aligned_buffer(alignment).get0();
                assert(dev.read(offset, check_buffer.get_write(), alignment).get0() == alignment);
                assert(std::memcmp(buffer.get() + offset, check_buffer.get(), alignment) == 0);
            });
        });

        when_all_succeed(std::move(write_fut), std::move(read_fut)).get0();

        // Check that writes were made in the correct places
        parallel_for_each(boost::irange<off_t>(0, blocks_num), [&](off_t block_no) {
            return async([&dev, &buffer, &new_buffer, &block_kind, block_no] {
                off_t offset = block_no * alignment;
                auto check_buffer = allocate_random_aligned_buffer(alignment).get0();
                assert(dev.read(offset, check_buffer.get_write(), alignment).get0() == alignment);
                auto& orig_buff = (block_kind[block_no] == WRITE ? new_buffer : buffer);
                assert(std::memcmp(orig_buff.get() + offset, check_buffer.get(), alignment) == 0);
            });
        }).get0();

        dev.close().get0();
    });
}

static future<> prepare_file(const std::string& file_path) {
    return async([&] {
        // Create device file if it does exist
        file dev = open_file_dma(file_path, open_flags::rw | open_flags::create).get0();

        auto st = dev.stat().get0();
        if (S_ISREG(st.st_mode) and st.st_size < min_device_size) {
            dev.truncate(min_device_size).get0();
        }

        dev.close().get0();
    });
}

int main(int argc, char** argv) {
    app_template app;
    app.add_options()
            ("help", "produce this help message")
            ("dev", boost::program_options::value<std::string>(),
                    "optional path to device file to test block_device on");
    return app.run(argc, argv, [&app] {
        return async([&] {
            auto& args = app.configuration();
            std::optional<temporary_file> tmp_device_file;
            std::string device_path = [&]() -> std::string {
                if (args.count("dev")) {
                    return args["dev"].as<std::string>();
                }

                tmp_device_file.emplace("/tmp/block_device_test_file");
                return tmp_device_file->path();
            }();

            assert(not device_path.empty());
            prepare_file(device_path).get0();
            test_basic_read_write(device_path).get0();
            test_parallel_read_write(device_path).get0();
            test_simultaneous_parallel_read_and_write(device_path).get0();
        });
    });
}
