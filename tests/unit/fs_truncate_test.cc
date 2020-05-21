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
#include "fs/cluster.hh"
#include "fs/metadata_log_operations/read.hh"
#include "fs/metadata_log_operations/truncate.hh"
#include "fs/metadata_log_operations/write.hh"
#include "fs/units.hh"
#include "fs_metadata_common.hh"
#include "fs_mock_block_device.hh"
#include "fs_mock_metadata_to_disk_buffer.hh"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/units.hh>
#include <seastar/testing/thread_test_case.hh>

#include <assert.h>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

using namespace seastar;
using namespace seastar::fs;

namespace {

constexpr unit_size_t default_cluster_size = 1 * MB;
constexpr unit_size_t default_alignment = 4096;
constexpr cluster_range default_cluster_range = {1, 10};
constexpr cluster_id_t default_metadata_log_cluster = 1;

auto default_init_metadata_log() {
    return init_metadata_log(default_cluster_size, default_alignment, default_metadata_log_cluster, default_cluster_range);
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_truncate_exceptions) {
    auto [blockdev, log] = default_init_metadata_log();
    inode_t inode = create_and_open_file(log);
    file_offset_t size = 3123;
    BOOST_CHECK_THROW(log.truncate(inode+1, size).get0(), invalid_inode_exception);
}

SEASTAR_THREAD_TEST_CASE(test_empty_truncate) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    const unix_time_t time_ns_start = get_current_time_ns();
    auto [blockdev, log] = default_init_metadata_log();
    inode_t inode = create_and_open_file(log);
    file_offset_t size = 3123;
    BOOST_TEST_MESSAGE("truncate size: " << size);
    log.truncate(inode, size).get0();
    auto meta_buff = get_current_metadata_buffer();
    BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

    // Check metadata
    BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), 2);
    ondisk_truncate expected_entry {
        inode,
        size,
        time_ns_start
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[1], expected_entry));

    // Check data
    temporary_buffer<uint8_t> buff = temporary_buffer<uint8_t>::aligned(default_alignment, round_up_to_multiple_of_power_of_2(size, default_alignment));
    temporary_buffer<uint8_t> read_buff = temporary_buffer<uint8_t>::aligned(default_alignment, round_up_to_multiple_of_power_of_2(size, default_alignment));
    memset(buff.get_write(), 0, size);
    BOOST_REQUIRE_EQUAL(log.read(inode, 0, read_buff.get_write(), size).get0(), size);
    BOOST_REQUIRE_EQUAL(memcmp(buff.get(), read_buff.get(), size), 0);

    BOOST_REQUIRE_EQUAL(log.file_size(inode), size);

    BOOST_TEST_MESSAGE("");
}

SEASTAR_THREAD_TEST_CASE(test_truncate_to_less) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 10;
    const unix_time_t time_ns_start = get_current_time_ns();
    small_write_len_t write_len = 3*default_alignment;
    file_offset_t size = 77;

    auto [blockdev, log] = default_init_metadata_log();
    inode_t inode = create_and_open_file(log);

    temporary_buffer<uint8_t> buff = temporary_buffer<uint8_t>::aligned(default_alignment, write_len);
    memset(buff.get_write(), 'a', write_len);
    BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), write_len).get0(), write_len);
    auto meta_buff = get_current_metadata_buffer();
    auto actions_size_after_write = meta_buff->actions.size();
    log.truncate(inode, size).get0();
    BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

    // Check metadata
    BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), actions_size_after_write+1);
    ondisk_truncate expected_entry {
        inode,
        size,
        time_ns_start
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[actions_size_after_write], expected_entry));

    // Check data
    temporary_buffer<uint8_t> read_buff = temporary_buffer<uint8_t>::aligned(default_alignment, write_len);
    BOOST_REQUIRE_EQUAL(log.read(inode, write_offset, read_buff.get_write(), size-write_offset).get0(), size-write_offset);
    BOOST_REQUIRE_EQUAL(memcmp(buff.get(), read_buff.get(), size-write_offset), 0);

    BOOST_REQUIRE_EQUAL(log.file_size(inode), size);

    BOOST_TEST_MESSAGE("");
}

SEASTAR_THREAD_TEST_CASE(test_truncate_to_more) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    constexpr file_offset_t write_offset = 10;
    const unix_time_t time_ns_start = get_current_time_ns();
    small_write_len_t write_len = 3*default_alignment;
    file_offset_t size = 3*default_alignment+default_alignment/3;

    auto [blockdev, log] = default_init_metadata_log();
    inode_t inode = create_and_open_file(log);

    temporary_buffer<uint8_t> buff = temporary_buffer<uint8_t>::aligned(default_alignment, write_len+default_alignment);
    memset(buff.get_write(), 0, size-write_offset);
    memset(buff.get_write(), 'a', write_len);
    BOOST_REQUIRE_EQUAL(log.write(inode, write_offset, buff.get(), write_len).get0(), write_len);
    auto meta_buff = get_current_metadata_buffer();
    auto actions_size_after_write = meta_buff->actions.size();
    log.truncate(inode, size).get0();
    BOOST_TEST_MESSAGE("meta_buff->actions: " << meta_buff->actions);

    // Check metadata
    BOOST_REQUIRE_EQUAL(meta_buff->actions.size(), actions_size_after_write+1);
    ondisk_truncate expected_entry {
        inode,
        size,
        time_ns_start
    };
    CHECK_CALL(check_metadata_entries_equal(meta_buff->actions[actions_size_after_write], expected_entry));

    // Check data
    temporary_buffer<uint8_t> read_buff = temporary_buffer<uint8_t>::aligned(default_alignment, write_len+default_alignment);
    BOOST_REQUIRE_EQUAL(log.read(inode, write_offset, read_buff.get_write(), size-write_offset).get0(), size-write_offset);
    BOOST_REQUIRE_EQUAL(memcmp(buff.get(), read_buff.get(), size-write_offset), 0);

    BOOST_REQUIRE_EQUAL(log.file_size(inode), size);

    BOOST_TEST_MESSAGE("");
}
