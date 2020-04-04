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

#include "fs_metadata_common.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_to_disk_buffer.hh"
#include "fs_mock_block_device.hh"

#include <cstring>
#include <seastar/core/units.hh>
#include <seastar/fs/block_device.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace seastar;
using namespace seastar::fs;

namespace {

constexpr auto APPENDED = metadata_to_disk_buffer::append_result::APPENDED;
constexpr auto TOO_BIG = metadata_to_disk_buffer::append_result::TOO_BIG;

constexpr unit_size_t max_siz = 1*MB;
constexpr unit_size_t alignment = 4*KB;

constexpr size_t necessary_bytes = sizeof(ondisk_type)+sizeof(ondisk_checkpoint)+sizeof(ondisk_type)+sizeof(ondisk_next_metadata_cluster);

temporary_buffer<uint8_t> tmp_buff_from_string(const char* str) {
    return temporary_buffer<uint8_t>(reinterpret_cast<const uint8_t*>(str), std::strlen(str));
}

struct test_data{
    shared_ptr<mock_block_device_impl> dev_impl;
    block_device dev;
    metadata_to_disk_buffer buf;
    temporary_buffer<uint8_t> tmp_buf;
    test_data() {
        dev_impl = make_shared<mock_block_device_impl>();
        dev = block_device(dev_impl);
        buf = metadata_to_disk_buffer();
        buf.init(max_siz, alignment, 0);
        tmp_buf = temporary_buffer<uint8_t>::aligned(alignment, max_siz);
    }
};

ondisk_small_write_header fill_header(uint16_t bytes) {
    ondisk_small_write_header fill_write {
        2,
        7,
        static_cast<uint16_t> (bytes + 1),
        17
    };
    return fill_write;
}

constexpr auto checkpoint_type = ondisk_type::CHECKPOINT;

}

SEASTAR_THREAD_TEST_CASE(test_too_big_next_metadata_cluster) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    ondisk_next_metadata_cluster next_metadata_cluster_op {2};
    size_t bytes_used = sizeof(ondisk_type) + sizeof(ondisk_checkpoint);
    while (bytes_used + ondisk_entry_size(next_metadata_cluster_op) <= max_siz) {
        BOOST_REQUIRE_EQUAL(test.buf.append(next_metadata_cluster_op), APPENDED);
        bytes_used += ondisk_entry_size(next_metadata_cluster_op);
    }
    BOOST_REQUIRE_EQUAL(test.buf.append(next_metadata_cluster_op), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_next_metadata_cluster) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    ondisk_type next_metadata_cluster_type = NEXT_METADATA_CLUSTER;
    ondisk_next_metadata_cluster next_metadata_cluster_op {2};
    BOOST_REQUIRE_EQUAL(test.buf.append(next_metadata_cluster_op), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_next_metadata_cluster), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &next_metadata_cluster_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &next_metadata_cluster_op, sizeof(ondisk_next_metadata_cluster)), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_create_inode) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_create_inode create_inode_op {42, 1, {5, 2, 6, 8, 4}};
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(create_inode_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(create_inode_op), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_create_inode) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    ondisk_type create_inode_type = CREATE_INODE;
    ondisk_create_inode create_inode_op {42, 1, {5, 2, 6, 8, 4}};
    BOOST_REQUIRE_EQUAL(test.buf.append(create_inode_op), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_create_inode), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &create_inode_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &create_inode_op, sizeof(ondisk_create_inode)), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_delete_inode) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_delete_inode delete_inode_op {1};
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(delete_inode_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(delete_inode_op), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_delete_inode) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    ondisk_type delete_inode_type = DELETE_INODE;
    ondisk_delete_inode delete_inode_op {1};
    BOOST_REQUIRE_EQUAL(test.buf.append(delete_inode_op), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_delete_inode), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &delete_inode_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &delete_inode_op, sizeof(ondisk_delete_inode)), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_small_write) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_small_write_header small_write_op {
        2,
        7,
        static_cast<uint16_t> (10),
        17
    };
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(small_write_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(small_write_op, fill_write_str.get()), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_small_write) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    temporary_buffer<uint8_t> small_write_str = tmp_buff_from_string("12345");
    ondisk_type small_write_type = SMALL_WRITE;
    ondisk_small_write_header small_write_op {
        2,
        7,
        static_cast<uint16_t>(small_write_str.size()),
        17
    };
    BOOST_REQUIRE_EQUAL(test.buf.append(small_write_op, small_write_str.get()), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_small_write_header) + small_write_str.size(), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &small_write_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &small_write_op, sizeof(ondisk_small_write_header)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type) + sizeof(ondisk_small_write_header), small_write_str.get(), small_write_str.size()), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_medium_write) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_medium_write medium_write_op {1, 8, 4, 6, 9};
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(medium_write_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(medium_write_op), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_medium_write) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    ondisk_type medium_write_type = MEDIUM_WRITE;
    ondisk_medium_write medium_write_op {1, 8, 4, 6, 9};
    BOOST_REQUIRE_EQUAL(test.buf.append(medium_write_op), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_medium_write), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &medium_write_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &medium_write_op, sizeof(ondisk_medium_write)), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_large_write) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_large_write large_write_op {6, 8, 2, 5};
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(large_write_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(large_write_op), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_large_write) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    ondisk_type large_write_type = LARGE_WRITE;
    ondisk_large_write large_write_op {6, 8, 2, 5};
    BOOST_REQUIRE_EQUAL(test.buf.append(large_write_op), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_large_write), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &large_write_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &large_write_op, sizeof(ondisk_large_write)), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_large_write_without_mtime) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_large_write_without_mtime large_write_without_mtime_op {256, 88, 11};
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(large_write_without_mtime_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(large_write_without_mtime_op), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_large_write_without_mtime) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    ondisk_type large_write_without_mtime_type = LARGE_WRITE_WITHOUT_MTIME;
    ondisk_large_write_without_mtime large_write_without_mtime_op {256, 88, 11};
    BOOST_REQUIRE_EQUAL(test.buf.append(large_write_without_mtime_op), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_large_write_without_mtime), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &large_write_without_mtime_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &large_write_without_mtime_op, sizeof(ondisk_large_write_without_mtime)), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_truncate) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_truncate truncate_op {64, 28, 62};
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(truncate_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(truncate_op), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_truncate) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    ondisk_type truncate_type = TRUNCATE;
    ondisk_truncate truncate_op {64, 28, 62};
    BOOST_REQUIRE_EQUAL(test.buf.append(truncate_op), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_truncate), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &truncate_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &truncate_op, sizeof(ondisk_truncate)), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_add_dir_entry) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_add_dir_entry_header add_dir_entry_op {
        2,
        7,
        static_cast<uint16_t>(10)
    };
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(add_dir_entry_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(add_dir_entry_op, fill_write_str.get()), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_add_dir_entry) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    temporary_buffer<uint8_t> add_dir_entry_str = tmp_buff_from_string("120345");
    ondisk_type add_dir_entry_type = ADD_DIR_ENTRY;
    ondisk_add_dir_entry_header add_dir_entry_op {
        2,
        7,
        static_cast<uint16_t>(add_dir_entry_str.size())
    };
    BOOST_REQUIRE_EQUAL(test.buf.append(add_dir_entry_op, add_dir_entry_str.get()), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_add_dir_entry_header) + add_dir_entry_str.size(), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &add_dir_entry_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &add_dir_entry_op, sizeof(ondisk_add_dir_entry_header)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type) + sizeof(ondisk_add_dir_entry_header), add_dir_entry_str.get(), add_dir_entry_str.size()), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_create_inode_as_dir_entry) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_create_inode_as_dir_entry_header create_inode_as_dir_entry_op {
        {
            42,
            1,
            {5, 2, 6, 8, 4}
        },
        7,
        static_cast<uint16_t>(10)
    };
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(create_inode_as_dir_entry_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(create_inode_as_dir_entry_op, fill_write_str.get()), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_create_inode_as_dir_entry) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    temporary_buffer<uint8_t> create_inode_as_dir_entry_str = tmp_buff_from_string("120345");
    ondisk_type create_inode_as_dir_entry_type = CREATE_INODE_AS_DIR_ENTRY;
    ondisk_create_inode_as_dir_entry_header create_inode_as_dir_entry_op {
        {
            42,
            1,
            {5, 2, 6, 8, 4}
        },
        7,
        static_cast<uint16_t>(create_inode_as_dir_entry_str.size())
    };
    BOOST_REQUIRE_EQUAL(test.buf.append(create_inode_as_dir_entry_op, create_inode_as_dir_entry_str.get()), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_create_inode_as_dir_entry_header) + create_inode_as_dir_entry_str.size(), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &create_inode_as_dir_entry_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &create_inode_as_dir_entry_op, sizeof(ondisk_create_inode_as_dir_entry_header)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type) + sizeof(ondisk_create_inode_as_dir_entry_header), 
        create_inode_as_dir_entry_str.get(), create_inode_as_dir_entry_str.size()), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_delete_dir_entry) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_delete_dir_entry_header delete_dir_entry_op {
        42,
        static_cast<uint16_t>(9)
    };
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(delete_dir_entry_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(delete_dir_entry_op, fill_write_str.get()), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_delete_dir_entry) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    temporary_buffer<uint8_t> delete_dir_entry_str = tmp_buff_from_string("120345");
    ondisk_type delete_dir_entry_type = DELETE_DIR_ENTRY;
    ondisk_delete_dir_entry_header delete_dir_entry_op {
        42,
        static_cast<uint16_t>(delete_dir_entry_str.size())
    };
    BOOST_REQUIRE_EQUAL(test.buf.append(delete_dir_entry_op, delete_dir_entry_str.get()), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_delete_dir_entry_header) + delete_dir_entry_str.size(), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &delete_dir_entry_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &delete_dir_entry_op, sizeof(ondisk_delete_dir_entry_header)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type) + sizeof(ondisk_delete_dir_entry_header), 
        delete_dir_entry_str.get(), delete_dir_entry_str.size()), 0);
}

SEASTAR_THREAD_TEST_CASE(test_too_big_delete_inode_and_dir_entry) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    test.buf.init_from_bootstrapped_cluster(max_siz, alignment, 0, max_siz-alignment);
    ondisk_delete_inode_and_dir_entry_header delete_inode_and_dir_entry_op {
        42,
        24,
        static_cast<uint16_t>(11)
    };
    auto fill_write_op = fill_header(alignment - necessary_bytes - sizeof(ondisk_type) - sizeof(ondisk_small_write_header) - ondisk_entry_size(delete_inode_and_dir_entry_op) + 1);
    auto fill_write_str = temporary_buffer<uint8_t>(fill_write_op.length);
    std::memset(fill_write_str.get_write(), fill_write_op.length, 'a');
    BOOST_REQUIRE_EQUAL(test.buf.append(fill_write_op, fill_write_str.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(test.buf.append(delete_inode_and_dir_entry_op, fill_write_str.get()), TOO_BIG);
}

SEASTAR_THREAD_TEST_CASE(test_delete_inode_and_dir_entry) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    test_data test{};
    temporary_buffer<uint8_t> delete_inode_and_dir_entry_str = tmp_buff_from_string("120345");
    ondisk_type delete_inode_and_dir_entry_type = DELETE_INODE_AND_DIR_ENTRY;
    ondisk_delete_inode_and_dir_entry_header delete_inode_and_dir_entry_op {
        42,
        24,
        static_cast<uint16_t>(delete_inode_and_dir_entry_str.size())
    };
    BOOST_REQUIRE_EQUAL(test.buf.append(delete_inode_and_dir_entry_op, delete_inode_and_dir_entry_str.get()), APPENDED);
    test.buf.flush_to_disk(test.dev).get();
    disk_offset_t len_aligned = round_up_to_multiple_of_power_of_2(sizeof(ondisk_type) + sizeof(ondisk_delete_inode_and_dir_entry_header) + delete_inode_and_dir_entry_str.size(), alignment);
    test.dev.read<uint8_t>(0, test.tmp_buf.get_write(), len_aligned).get();
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &checkpoint_type, sizeof(ondisk_type)), 0);
    test.tmp_buf.trim_front(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get(), &delete_inode_and_dir_entry_type, sizeof(ondisk_type)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type), &delete_inode_and_dir_entry_op, sizeof(ondisk_delete_inode_and_dir_entry_header)), 0);
    BOOST_REQUIRE_EQUAL(std::memcmp(test.tmp_buf.get() + sizeof(ondisk_type) + sizeof(ondisk_delete_inode_and_dir_entry_header), 
        delete_inode_and_dir_entry_str.get(), delete_inode_and_dir_entry_str.size()), 0);
}
