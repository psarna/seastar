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

#pragma once

#include "fs/bitwise.hh"
#include "fs/cluster.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "fs/metadata_log_operations/write.hh"
#include "fs_mock_block_device.hh"
#include "fs_mock_cluster_writer.hh"
#include "fs_mock_metadata_to_disk_buffer.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/fs/units.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>

#include <assert.h>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#define CHECK_CALL(...) BOOST_TEST_CONTEXT("Called from line: " << __LINE__) { __VA_ARGS__; }

// Returns copy of the given value. Needed to solve misalignment problems.
template<typename T>
inline T copy_value(T x) {
    return x;
}

namespace seastar {

inline std::ostream& operator<<(std::ostream& os, const temporary_buffer<uint8_t>& data) {
    constexpr size_t MAX_ELEMENTS_PRINTED = 10;
    for (size_t i = 0; i < std::min(MAX_ELEMENTS_PRINTED, data.size()); ++i) {
        if (isprint(data[i]) != 0) {
            os << data[i];
        } else {
            os << '<' << (int)data[i] << '>';
        }
    }
    if (data.size() > MAX_ELEMENTS_PRINTED) {
        os << "...(size=" << data.size() << ')';
    }
    return os;
}

} // seastar

namespace seastar::fs {

template<typename ActionType>
const auto is_append_type = mock_metadata_to_disk_buffer::is_append_type<ActionType>;
template<typename ActionType>
const auto is_type = mock_metadata_to_disk_buffer::is_type<ActionType>;
template<typename ActionType>
const auto get_by_append_type = mock_metadata_to_disk_buffer::get_by_append_type<ActionType>;
template<typename ActionType>
const auto get_by_type = mock_metadata_to_disk_buffer::get_by_type<ActionType>;

using flush_to_disk_action = mock_metadata_to_disk_buffer::action::flush_to_disk;
using append_action = mock_metadata_to_disk_buffer::action::append;

constexpr auto SMALL_WRITE_THRESHOLD = write_operation::SMALL_WRITE_THRESHOLD;

using medium_write_len_t = decltype(ondisk_medium_write::length);
using small_write_len_t = decltype(ondisk_small_write_header::length);
using unix_time_t = decltype(unix_metadata::mtime_ns);

template<typename BlockDevice = mock_block_device_impl,
        typename MetadataToDiskBuffer = mock_metadata_to_disk_buffer,
        typename ClusterWriter = mock_cluster_writer>
inline auto init_metadata_log(unit_size_t cluster_size, unit_size_t alignment, cluster_id_t metadata_log_cluster,
        cluster_range cluster_range, double compactness, size_t max_data_compaction_memory) {
    auto dev_impl = make_shared<BlockDevice>();
    metadata_log log(block_device(dev_impl), cluster_size, alignment,
            make_shared<MetadataToDiskBuffer>(), make_shared<ClusterWriter>(),
            compactness, max_data_compaction_memory);
    log.bootstrap(0, metadata_log_cluster, cluster_range, 1, 0).get();

    return std::pair{std::move(dev_impl), std::move(log)};
}

inline auto& get_current_metadata_buffer() {
    auto& created_buffers = mock_metadata_to_disk_buffer::virtually_constructed_buffers;
    assert(created_buffers.size() > 0);
    return created_buffers.back();
}

inline auto& get_current_cluster_writer() {
    auto& created_writers = mock_cluster_writer::virtually_constructed_writers;
    assert(created_writers.size() > 0);
    return created_writers.back();
}

inline inode_t create_and_open_file(metadata_log& log, std::string name = "tmp") {
    log.create_file("/" + name, file_permissions::default_dir_permissions).get0();
    return log.open_file("/" + name).get0();
}

inline temporary_buffer<uint8_t> gen_buffer(size_t len, bool aligned, unit_size_t alignment) {
    std::default_random_engine random_engine(testing::local_random_engine());
    temporary_buffer<uint8_t> buff;
    if (not aligned) {
        // Make buff unaligned
        buff = temporary_buffer<uint8_t>::aligned(alignment, round_up_to_multiple_of_power_of_2(len, alignment) + alignment);
        size_t offset = std::uniform_int_distribution<>(1, alignment - 1)(random_engine);
        buff.trim_front(offset);
    } else {
        buff = temporary_buffer<uint8_t>::aligned(alignment, round_up_to_multiple_of_power_of_2(len, alignment));
    }
    buff.trim(len);

    for (auto [i, char_dis] = std::tuple((size_t)0, std::uniform_int_distribution<>(0, 256)); i < buff.size(); i++) {
        buff.get_write()[i] = char_dis(random_engine);
    }

    return buff;
}

inline void aligned_write(metadata_log& log, inode_t inode, size_t bytes_num, unit_size_t alignment) {
    assert(bytes_num % alignment == 0);
    temporary_buffer<uint8_t> buff = gen_buffer(bytes_num, true, alignment);
    size_t wrote = log.write(inode, 0, buff.get(), buff.size()).get0();
    BOOST_REQUIRE_EQUAL(wrote, buff.size());
}

inline unix_time_t get_current_time_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

inline bool operator==(const ondisk_unix_metadata& l, const ondisk_unix_metadata& r) {
    return std::memcmp(&l, &r, sizeof(l)) == 0;
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_unix_metadata& metadata) {
    os << "[" << metadata.perms << ",";
    os << metadata.uid << ",";
    os << metadata.gid << ",";
    os << metadata.btime_ns << ",";
    os << metadata.mtime_ns << ",";
    os << metadata.ctime_ns;
    return os << "]";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_next_metadata_cluster& entry) {
    os << "{" << "cluster_id=" << entry.cluster_id;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_create_inode& entry) {
    os << "{" << "inode=" << entry.inode;
    os << ", is_directory=" << (int)entry.is_directory;
    // os << ", metadata=" << entry.metadata;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_delete_inode& entry) {
    os << "{" << "inode=" << entry.inode;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_small_write_header& entry) {
    os << "{" << "inode=" << entry.inode;
    os << ", offset=" << entry.offset;
    os << ", length=" << entry.length;
    // os << ", time_ns=" << entry.time_ns;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_medium_write& entry) {
    os << "{" << "inode=" << entry.inode;
    os << ", offset=" << entry.offset;
    os << ", disk_offset=" << entry.disk_offset;
    os << ", length=" << entry.length;
    // os << ", time_ns=" << entry.time_ns;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_large_write& entry) {
    os << "{" << "inode=" << entry.inode;
    os << ", offset=" << entry.offset;
    os << ", data_cluster=" << entry.data_cluster;
    // os << ", time_ns=" << entry.time_ns;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_large_write_without_mtime& entry) {
    os << "{" << "inode=" << entry.inode;
    os << ", offset=" << entry.offset;
    os << ", data_cluster=" << entry.data_cluster;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_truncate& entry) {
    os << "{" << "inode=" << entry.inode;
    os << ", size=" << entry.size;
    // os << ", time_ns=" << entry.time_ns;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_add_dir_entry_header& entry) {
    os << "{" << "dir_inode=" << entry.dir_inode;
    os << ", entry_inode=" << entry.entry_inode;
    os << ", entry_name_length=" << entry.entry_name_length;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_create_inode_as_dir_entry_header& entry) {
    os << "{" << "entry_inode=" << entry.entry_inode;
    os << ", dir_inode=" << entry.dir_inode;
    os << ", entry_name_length=" << entry.entry_name_length;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_delete_dir_entry_header& entry) {
    os << "{" << "dir_inode=" << entry.dir_inode;
    os << ", entry_name_length=" << entry.entry_name_length;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_delete_inode_and_dir_entry_header& entry) {
    os << "{" << "inode_to_delete=" << entry.inode_to_delete;
    os << ", dir_inode=" << entry.dir_inode;
    os << ", entry_name_length=" << entry.entry_name_length;
    return os << "}";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_small_write& entry) {
    os << "(header=" << entry.header;
    os << ", data=" << entry.data;
    return os << ")";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_add_dir_entry& entry) {
    os << "(header=" << entry.header;
    os << ", entry_name=" << entry.entry_name;
    return os << ")";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_create_inode_as_dir_entry& entry) {
    os << "(header=" << entry.header;
    os << ", entry_name=" << entry.entry_name;
    return os << ")";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_delete_dir_entry& entry) {
    os << "(header=" << entry.header;
    os << ", entry_name=" << entry.entry_name;
    return os << ")";
}

inline std::ostream& operator<<(std::ostream& os, const ondisk_delete_inode_and_dir_entry& entry) {
    os << "(header=" << entry.header;
    os << ", entry_name=" << entry.entry_name;
    return os << ")";
}

inline std::ostream& operator<<(std::ostream& os, const mock_metadata_to_disk_buffer::action& action) {
    std::visit(overloaded {
        [&os](const mock_metadata_to_disk_buffer::action::append& append) {
            os << "[append:";
            std::visit(overloaded {
                [&os](const ondisk_next_metadata_cluster& entry) {
                    os << "next_metadata_cluster=" << entry;
                },
                [&os](const ondisk_create_inode& entry) {
                    os << "create_inode=" << entry;
                },
                [&os](const ondisk_delete_inode& entry) {
                    os << "delete_inode=" << entry;
                },
                [&os](const ondisk_small_write& entry) {
                    os << "small_write=" << entry;
                },
                [&os](const ondisk_medium_write& entry) {
                    os << "medium_write=" << entry;
                },
                [&os](const ondisk_large_write& entry) {
                    os << "large_write=" << entry;
                },
                [&os](const ondisk_large_write_without_mtime& entry) {
                    os << "large_write_without_mtime=" << entry;
                },
                [&os](const ondisk_truncate& entry) {
                    os << "truncate=" << entry;
                },
                [&os](const ondisk_add_dir_entry& entry) {
                    os << "add_dir_entry=" << entry;
                },
                [&os](const ondisk_create_inode_as_dir_entry& entry) {
                    os << "create_inode_as_dir_entry=" << entry;
                },
                [&os](const ondisk_delete_dir_entry& entry) {
                    os << "delete_dir_entry=" << entry;
                },
                [&os](const ondisk_delete_inode_and_dir_entry& entry) {
                    os << "delete_inode_and_dir_entry=" << entry;
                }
            }, append.entry);
            os << "]";
        },
        [&os](const mock_metadata_to_disk_buffer::action::flush_to_disk&) {
            os << "[flush]";
        }
    }, action.data);
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const std::vector<mock_metadata_to_disk_buffer::action>& actions) {
    for (size_t i = 0; i < actions.size(); ++i) {
        if (i != 0) {
            os << '\n';
        }
        os << actions[i];
    }
    return os;
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_next_metadata_cluster& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_next_metadata_cluster>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_next_metadata_cluster>(given_action);
    BOOST_CHECK_EQUAL(copy_value(given_entry.cluster_id), copy_value(expected_entry.cluster_id));
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_create_inode& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_create_inode>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_create_inode>(given_action);
    BOOST_CHECK_EQUAL(copy_value(given_entry.inode), copy_value(expected_entry.inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.is_directory), copy_value(expected_entry.is_directory));
    BOOST_CHECK_EQUAL(copy_value(given_entry.metadata), copy_value(expected_entry.metadata));
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_delete_inode& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_delete_inode>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_delete_inode>(given_action);
    BOOST_CHECK_EQUAL(copy_value(given_entry.inode), copy_value(expected_entry.inode));
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_small_write& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_small_write>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_small_write>(given_action);
    BOOST_CHECK_EQUAL(given_entry.data, expected_entry.data);
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.inode), copy_value(expected_entry.header.inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.offset), copy_value(expected_entry.header.offset));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.length), copy_value(expected_entry.header.length));
    BOOST_CHECK_GE(copy_value(given_entry.header.time_ns), copy_value(expected_entry.header.time_ns));
    BOOST_CHECK_LE(copy_value(given_entry.header.time_ns), get_current_time_ns());
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_medium_write& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_medium_write>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_medium_write>(given_action);
    BOOST_CHECK_EQUAL(copy_value(given_entry.inode), copy_value(expected_entry.inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.offset), copy_value(expected_entry.offset));
    BOOST_CHECK_EQUAL(copy_value(given_entry.disk_offset), copy_value(expected_entry.disk_offset));
    BOOST_CHECK_EQUAL(copy_value(given_entry.length), copy_value(expected_entry.length));
    BOOST_CHECK_GE(copy_value(given_entry.time_ns), copy_value(expected_entry.time_ns));
    BOOST_CHECK_LE(copy_value(given_entry.time_ns), get_current_time_ns());
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_large_write& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_large_write>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_large_write>(given_action);
    BOOST_CHECK_EQUAL(copy_value(given_entry.inode), copy_value(expected_entry.inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.offset), copy_value(expected_entry.offset));
    BOOST_CHECK_EQUAL(copy_value(given_entry.data_cluster), copy_value(expected_entry.data_cluster));
    BOOST_CHECK_GE(copy_value(given_entry.time_ns), copy_value(expected_entry.time_ns));
    BOOST_CHECK_LE(copy_value(given_entry.time_ns), get_current_time_ns());
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_large_write_without_mtime& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_large_write_without_mtime>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_large_write_without_mtime>(given_action);
    BOOST_CHECK_EQUAL(copy_value(given_entry.inode), copy_value(expected_entry.inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.offset), copy_value(expected_entry.offset));
    BOOST_CHECK_EQUAL(copy_value(given_entry.data_cluster), copy_value(expected_entry.data_cluster));
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_truncate& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_truncate>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_truncate>(given_action);
    BOOST_CHECK_EQUAL(copy_value(given_entry.inode), copy_value(expected_entry.inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.size), copy_value(expected_entry.size));
    BOOST_CHECK_GE(copy_value(given_entry.time_ns), copy_value(expected_entry.time_ns));
    BOOST_CHECK_LE(copy_value(given_entry.time_ns), get_current_time_ns());
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_add_dir_entry& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_add_dir_entry>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_add_dir_entry>(given_action);
    BOOST_CHECK_EQUAL(given_entry.entry_name, expected_entry.entry_name);
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.dir_inode), copy_value(expected_entry.header.dir_inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.entry_inode), copy_value(expected_entry.header.entry_inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.entry_name_length), copy_value(expected_entry.header.entry_name_length));
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_create_inode_as_dir_entry& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_create_inode_as_dir_entry>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_create_inode_as_dir_entry>(given_action);
    BOOST_CHECK_EQUAL(given_entry.entry_name, expected_entry.entry_name);
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.entry_inode.inode), copy_value(expected_entry.header.entry_inode.inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.entry_inode.is_directory),
            copy_value(expected_entry.header.entry_inode.is_directory));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.entry_inode.metadata),
            copy_value(expected_entry.header.entry_inode.metadata));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.dir_inode), copy_value(expected_entry.header.dir_inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.entry_name_length), copy_value(expected_entry.header.entry_name_length));
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_delete_dir_entry& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_delete_dir_entry>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_delete_dir_entry>(given_action);
    BOOST_CHECK_EQUAL(given_entry.entry_name, expected_entry.entry_name);
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.dir_inode), copy_value(expected_entry.header.dir_inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.entry_name_length), copy_value(expected_entry.header.entry_name_length));
}

inline void check_metadata_entries_equal(const mock_metadata_to_disk_buffer::action& given_action,
        const ondisk_delete_inode_and_dir_entry& expected_entry) {
    BOOST_REQUIRE(mock_metadata_to_disk_buffer::is_append_type<ondisk_delete_inode_and_dir_entry>(given_action));
    auto& given_entry = mock_metadata_to_disk_buffer::get_by_append_type<ondisk_delete_inode_and_dir_entry>(given_action);
    BOOST_CHECK_EQUAL(given_entry.entry_name, expected_entry.entry_name);
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.inode_to_delete), copy_value(expected_entry.header.inode_to_delete));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.dir_inode), copy_value(expected_entry.header.dir_inode));
    BOOST_CHECK_EQUAL(copy_value(given_entry.header.entry_name_length), copy_value(expected_entry.header.entry_name_length));
}

} // seastar::fs
