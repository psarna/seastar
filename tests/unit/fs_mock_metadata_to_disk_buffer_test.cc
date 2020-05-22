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

#include "fs/backend/shard.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_to_disk_buffer.hh"
#include "fs_metadata_common.hh"
#include "fs_mock_block_device.hh"
#include "fs_mock_metadata_to_disk_buffer.hh"

#include <seastar/core/print.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/units.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>
#include <cstdint>

using namespace seastar;
using namespace seastar::fs;

namespace {

constexpr auto APPENDED = mock_metadata_to_disk_buffer::APPENDED;
constexpr auto TOO_BIG = mock_metadata_to_disk_buffer::TOO_BIG;

constexpr size_t default_buff_size = 1 * MB;
constexpr unit_size_t default_alignment = 4 * KB;

template<typename MetadataToDiskBuffer>
shared_ptr<MetadataToDiskBuffer> create_metadata_buffer() {
    auto buf = make_shared<MetadataToDiskBuffer>();
    buf->init(default_buff_size, default_alignment, 0);
    return buf;
}

temporary_buffer<uint8_t> tmp_buff_from_string(const char* str) {
    return temporary_buffer<uint8_t>(reinterpret_cast<const uint8_t*>(str), std::strlen(str));
}

} // namespace

// The following test checks if exceeding append is properly handled
SEASTAR_THREAD_TEST_CASE(size_exceeded_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    ondisk_delete_inode entry {1};
    for (;;) {
        if (buf->append(entry) == APPENDED) {
            BOOST_REQUIRE_EQUAL(mock_buf->append(entry), APPENDED);
        } else {
            BOOST_REQUIRE_EQUAL(mock_buf->append(entry), TOO_BIG);
            BOOST_REQUIRE_EQUAL(buf->bytes_left(), mock_buf->bytes_left());
            break;
        }
        BOOST_REQUIRE_EQUAL(buf->bytes_left(), mock_buf->bytes_left());
    }
}

// The following test checks if multiple actions data are correctly added to actions vector
SEASTAR_THREAD_TEST_CASE(actions_index_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);

    BOOST_REQUIRE_EQUAL(mock_buf->append(ondisk_next_metadata_cluster {1}), APPENDED);
    mock_buf->flush_to_disk(dev).get();
    BOOST_REQUIRE_EQUAL(mock_buf->append(ondisk_create_inode {4, {1, 5, 2, 6, 8, 4}}), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(ondisk_delete_inode {1}), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(ondisk_medium_write {1, 8, 4, 6, 9}), APPENDED);
    mock_buf->flush_to_disk(dev).get();
    BOOST_REQUIRE_EQUAL(mock_buf->append(ondisk_large_write {6, 8, 2, 5}), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(ondisk_large_write_without_mtime {6, 8, 1}), APPENDED);
    mock_buf->flush_to_disk(dev).get();
    BOOST_REQUIRE_EQUAL(mock_buf->append(ondisk_truncate {64, 28, 62}), APPENDED);
    mock_buf->flush_to_disk(dev).get();

    auto& actions = mock_buf->actions;
    BOOST_REQUIRE(is_append_type<ondisk_next_metadata_cluster>(actions[0]));
    BOOST_REQUIRE(is_type<flush_to_disk_action>(actions[1]));
    BOOST_REQUIRE(is_append_type<ondisk_create_inode>(actions[2]));
    BOOST_REQUIRE(is_append_type<ondisk_delete_inode>(actions[3]));
    BOOST_REQUIRE(is_append_type<ondisk_medium_write>(actions[4]));
    BOOST_REQUIRE(is_type<flush_to_disk_action>(actions[5]));
    BOOST_REQUIRE(is_append_type<ondisk_large_write>(actions[6]));
    BOOST_REQUIRE(is_append_type<ondisk_large_write_without_mtime>(actions[7]));
    BOOST_REQUIRE(is_type<flush_to_disk_action>(actions[8]));
    BOOST_REQUIRE(is_append_type<ondisk_truncate>(actions[9]));
    BOOST_REQUIRE(is_type<flush_to_disk_action>(actions[10]));
}

// The folowing test checks that constructed buffers are distinct and correctly added to
// virtually_constructed_buffers vector
SEASTAR_THREAD_TEST_CASE(virtual_constructor_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto& created_buffers = mock_metadata_to_disk_buffer::virtually_constructed_buffers;

    auto mock_buf0 = mock_metadata_to_disk_buffer().virtual_constructor();
    mock_buf0->init(default_buff_size, default_alignment, 0);
    BOOST_REQUIRE_EQUAL(created_buffers.size(), 1);
    BOOST_REQUIRE(created_buffers[0] == mock_buf0);

    auto mock_buf1 = mock_buf0->virtual_constructor();
    mock_buf1->init(default_buff_size, default_alignment, 0);
    BOOST_REQUIRE_EQUAL(created_buffers.size(), 2);
    BOOST_REQUIRE(created_buffers[1] == mock_buf1);

    auto mock_buf2 = mock_buf1->virtual_constructor();
    mock_buf2->init(default_buff_size, default_alignment, 0);
    BOOST_REQUIRE_EQUAL(created_buffers.size(), 3);
    BOOST_REQUIRE(created_buffers[2] == mock_buf2);


    BOOST_REQUIRE_EQUAL(created_buffers[0]->actions.size(), 0);

    BOOST_REQUIRE_EQUAL(mock_buf1->append(ondisk_delete_inode {1}), APPENDED);
    BOOST_REQUIRE_EQUAL(created_buffers[1]->actions.size(), 1);

    BOOST_REQUIRE_EQUAL(mock_buf2->append(ondisk_delete_inode {1}), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf2->append(ondisk_delete_inode {1}), APPENDED);
    BOOST_REQUIRE_EQUAL(created_buffers[2]->actions.size(), 2);
}

// Tests below check that actions add correct info to actions vector

SEASTAR_THREAD_TEST_CASE(flush_action_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);

    mock_buf->flush_to_disk(dev).get();

    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_type<flush_to_disk_action>(mock_buf->actions[0]));
}

SEASTAR_THREAD_TEST_CASE(next_metadata_cluster_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    ondisk_next_metadata_cluster next_metadata_cluster_op {6};
    BOOST_REQUIRE_EQUAL(buf->append(next_metadata_cluster_op), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(next_metadata_cluster_op), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], next_metadata_cluster_op));
}

SEASTAR_THREAD_TEST_CASE(create_inode_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    ondisk_create_inode create_inode_op {42, {1, 5, 2, 6, 8, 4}};
    BOOST_REQUIRE_EQUAL(buf->append(create_inode_op), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(create_inode_op), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], create_inode_op));
}

SEASTAR_THREAD_TEST_CASE(delete_inode_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    ondisk_delete_inode delete_inode_op {1};
    BOOST_REQUIRE_EQUAL(buf->append(delete_inode_op), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(delete_inode_op), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], delete_inode_op));
}

SEASTAR_THREAD_TEST_CASE(small_write_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    temporary_buffer<uint8_t> small_write_str = tmp_buff_from_string("12345");
    ondisk_small_write small_write_op {
        {
            2,
            7,
            static_cast<uint16_t>(small_write_str.size()),
            17
        },
        small_write_str.share()
    };
    BOOST_REQUIRE_EQUAL(buf->append(small_write_op.header, small_write_op.data.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(small_write_op.header, small_write_op.data.get()), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], small_write_op));
}

SEASTAR_THREAD_TEST_CASE(medium_write_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    ondisk_medium_write medium_write_op {1, 8, 4, 6, 9};
    BOOST_REQUIRE_EQUAL(buf->append(medium_write_op), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(medium_write_op), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], medium_write_op));
}

SEASTAR_THREAD_TEST_CASE(large_write_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    ondisk_large_write large_write_op {6, 8, 2, 5};
    BOOST_REQUIRE_EQUAL(buf->append(large_write_op), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(large_write_op), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], large_write_op));
}

SEASTAR_THREAD_TEST_CASE(large_write_without_mtime_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    ondisk_large_write_without_mtime large_write_op {256, 88, 11};
    BOOST_REQUIRE_EQUAL(buf->append(large_write_op), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(large_write_op), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], large_write_op));
}

SEASTAR_THREAD_TEST_CASE(truncate_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    ondisk_truncate truncate_op {64, 28, 62};
    BOOST_REQUIRE_EQUAL(buf->append(truncate_op), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(truncate_op), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], truncate_op));
}

SEASTAR_THREAD_TEST_CASE(add_dir_entry_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    temporary_buffer<uint8_t> add_dir_entry_str = tmp_buff_from_string("120345");
    ondisk_add_dir_entry add_dir_entry_op {
        {
            2,
            7,
            static_cast<uint16_t>(add_dir_entry_str.size())
        },
        add_dir_entry_str.share()
    };
    BOOST_REQUIRE_EQUAL(buf->append(add_dir_entry_op.header, add_dir_entry_op.entry_name.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(add_dir_entry_op.header, add_dir_entry_op.entry_name.get()), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], add_dir_entry_op));
}

SEASTAR_THREAD_TEST_CASE(create_inode_as_dir_entry_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    temporary_buffer<uint8_t> create_inode_str = tmp_buff_from_string("120345");
    ondisk_create_inode_as_dir_entry create_inode_op {
        {
            {
                42,
                {1, 5, 2, 6, 8, 4}
            },
            7,
            static_cast<uint16_t>(create_inode_str.size())
        },
        create_inode_str.share()
    };
    BOOST_REQUIRE_EQUAL(buf->append(create_inode_op.header, create_inode_op.entry_name.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(create_inode_op.header, create_inode_op.entry_name.get()), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], create_inode_op));
}

SEASTAR_THREAD_TEST_CASE(delete_dir_entry_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    auto mock_buf = create_metadata_buffer<mock_metadata_to_disk_buffer>();
    auto buf = create_metadata_buffer<metadata_to_disk_buffer>();

    temporary_buffer<uint8_t> delete_entry_str = tmp_buff_from_string("120345");
    ondisk_delete_dir_entry delete_entry_op {
        {
            42,
            static_cast<uint16_t>(delete_entry_str.size())
        },
        delete_entry_str.share()
    };
    BOOST_REQUIRE_EQUAL(buf->append(delete_entry_op.header, delete_entry_op.entry_name.get()), APPENDED);
    BOOST_REQUIRE_EQUAL(mock_buf->append(delete_entry_op.header, delete_entry_op.entry_name.get()), APPENDED);

    BOOST_REQUIRE_EQUAL(mock_buf->bytes_left(), buf->bytes_left());

    BOOST_REQUIRE_EQUAL(mock_buf->actions.size(), 1);
    CHECK_CALL(check_metadata_entries_equal(mock_buf->actions[0], delete_entry_op));
}
