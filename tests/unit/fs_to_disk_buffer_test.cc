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

#include "fs/bitwise.hh"
#include "fs/to_disk_buffer.hh"
#include "fs/units.hh"
#include "fs_mock_block_device.hh"

#include <cstring>
#include <seastar/core/units.hh>
#include <seastar/fs/block_device.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace seastar;
using namespace seastar::fs;

constexpr unit_size_t alignment = 4*KB;
constexpr unit_size_t max_siz = 32*MB; // reasonably larger than alignment

BOOST_TEST_DONT_PRINT_LOG_VALUE(to_disk_buffer)

SEASTAR_THREAD_TEST_CASE(test_initially_empty) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer();
    disk_buf.init(max_siz, alignment, 0);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
}

SEASTAR_THREAD_TEST_CASE(test_simple_write) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer();
    disk_buf.init(max_siz, alignment, 0);
    auto buf = temporary_buffer<char>::aligned(alignment, max_siz);
    auto expected_buf = temporary_buffer<char>::aligned(alignment, max_siz);
    disk_offset_t len_aligned;
    disk_buf.append_bytes("12345678", 6);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-6);
    disk_buf.append_bytes("abcdefghi", 9);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-15);
    disk_buf.flush_to_disk(dev).get();
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)15, alignment);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-len_aligned);
    dev.read<char>(0, buf.get_write(), len_aligned).get();
    strncpy(expected_buf.get_write(), "123456abcdefghi", max_siz);  // fills with null characters
    BOOST_REQUIRE_EQUAL(std::memcmp(expected_buf.get(), buf.get(), len_aligned), 0);
}

SEASTAR_THREAD_TEST_CASE(test_multiple_write) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer();
    disk_buf.init(max_siz, alignment, 0);
    auto buf = temporary_buffer<char>::aligned(alignment, max_siz);
    auto expected_buf = temporary_buffer<char>::aligned(alignment, max_siz);
    disk_offset_t len_aligned, len_aligned2;
    disk_buf.append_bytes("9876", 4);
    disk_buf.append_bytes("zyxwvutsr", 9);
    disk_buf.flush_to_disk(dev).get();
    len_aligned = round_up_to_multiple_of_power_of_2((disk_offset_t)13, alignment);
    dev.read<char>(0, buf.get_write(), len_aligned).get();
    strncpy(expected_buf.get_write(), "9876zyxwvutsr", max_siz);
    BOOST_REQUIRE_EQUAL(std::memcmp(expected_buf.get(), buf.get(), len_aligned), 0);

    disk_buf.append_bytes("12345678", 6);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-len_aligned-6);
    disk_buf.append_bytes("abcdefghi", 9);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-len_aligned-15);
    disk_buf.flush_to_disk(dev).get();
    len_aligned2 = round_up_to_multiple_of_power_of_2(len_aligned+15, alignment);
    strncpy(expected_buf.get_write()+len_aligned, "123456abcdefghi", len_aligned2-len_aligned);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz-len_aligned2);
    dev.read<char>(0, buf.get_write(), len_aligned2).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(expected_buf.get(), buf.get(), len_aligned2), 0);
}

SEASTAR_THREAD_TEST_CASE(test_empty_write) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer();
    disk_buf.init(max_siz, alignment, 0);
    disk_buf.flush_to_disk(dev).get();
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
}

SEASTAR_THREAD_TEST_CASE(test_empty_append_bytes) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer();
    disk_buf.init(max_siz, alignment, 0);
    disk_buf.append_bytes("123456", 0);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
    disk_buf.flush_to_disk(dev).get();
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz);
}

SEASTAR_THREAD_TEST_CASE(test_combined) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    auto disk_buf = to_disk_buffer();
    disk_buf.init(max_siz, alignment, 0);
    auto buf = temporary_buffer<char>::aligned(alignment, max_siz);
    auto inp = temporary_buffer<char>::aligned(alignment, max_siz);
    auto expected_buf = temporary_buffer<char>::aligned(alignment, max_siz);
    strncpy(inp.get_write(), "abcdefghij12345678**987654****", max_siz); // fills to max_siz with null characters
    disk_offset_t beg = 0, end = 0;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    disk_buf.append_bytes(inp.get(), 10);
    end += 10;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    disk_buf.flush_to_disk(dev).get();
    end = round_up_to_multiple_of_power_of_2(end, alignment);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    dev.read<char>(beg, buf.get_write(), end-beg).get();
    strncpy(expected_buf.get_write(), "abcdefghij", end-beg);
    BOOST_REQUIRE_EQUAL(std::memcmp(expected_buf.get(), buf.get(), end-beg), 0);

    beg = end;
    disk_buf.append_bytes(inp.get()+10, 8);
    end += 8;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    disk_buf.append_bytes(inp.get()+20, 6);
    end += 6;
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    disk_buf.flush_to_disk(dev).get();
    end = round_up_to_multiple_of_power_of_2(end, alignment);
    BOOST_REQUIRE_EQUAL(disk_buf.bytes_left(), max_siz - end);

    dev.read<char>(beg, buf.get_write(), end-beg).get();
    std::memset(expected_buf.get_write(), 0, end-beg);
    strncpy(expected_buf.get_write(), inp.get()+10, 8);
    strncpy(expected_buf.get_write()+8, inp.get()+20, 6);
    BOOST_REQUIRE_EQUAL(std::memcmp(expected_buf.get(), buf.get(), end-beg), 0);
}
