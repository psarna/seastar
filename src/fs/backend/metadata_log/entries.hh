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

#include "fs/unix_metadata.hh"
#include "fs/val_or_err.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/fs/unit_types.hh"

#include <string>
#include <string_view>

namespace seastar::fs::backend::metadata_log::entries {

struct checkpoint {
    // The disk format is as follows:
    // | checkpoint | .............................. |
    //              |             data               |
    //              |<-- checkpointed_data_length -->|
    //                                               ^
    //       _______________________________________/
    //      /
    //    there ends checkpointed data and (next checkpoint begins or metadata in the current cluster end)
    //
    // CRC is calculated from byte sequence | data | checkpointed_data_length |
    // E.g. if the data consist of bytes "abcd" and checkpointed_data_length of bytes "xyz" then the byte sequence
    // would be "abcdxyz"
    uint32_t crc32_checksum;
    uint32_t checkpointed_data_length;
};

struct next_metadata_cluster {
    cluster_id_t cluster_id; // metadata log continues there
};

struct create_inode {
    inode_t inode;
    unix_metadata metadata;
};

struct delete_inode {
    inode_t inode;
};

struct small_write {
    static constexpr size_t data_max_len = 65535;

    inode_t inode;
    file_offset_t offset;
    time_ns_t time_ns;
    temporary_buffer<uint8_t> data;
};

struct medium_write {
    inode_t inode;
    file_offset_t offset;
    disk_range drange; // length < cluster_size
    time_ns_t time_ns;
};

struct large_write_without_time {
    inode_t inode;
    file_offset_t offset;
    cluster_id_t data_cluster; // data length == cluster_size
};

struct large_write {
    large_write_without_time lwwt;
    time_ns_t time_ns;
};

struct truncate {
    inode_t inode;
    file_offset_t size;
    time_ns_t time_ns;
};

constexpr size_t dentry_name_max_len = 65535;

struct create_dentry {
    inode_t inode;
    std::string name;
    inode_t dir_inode;
    // TODO: Maybe time_ns for modifying directory?
};

struct create_inode_as_dentry {
    create_inode inode;
    std::string name;
    inode_t dir_inode;
    // TODO: Maybe time_ns for modifying directory?
};

struct delete_dentry {
    inode_t dir_inode;
    std::string name;
    // TODO: Maybe time_ns for modifying directory?
};

struct delete_inode_and_dentry {
    delete_inode di;
    delete_dentry dd;
};

template<typename> disk_offset_t ondisk_size() noexcept = delete;
template<typename> disk_offset_t ondisk_size(size_t) noexcept = delete;

template<> disk_offset_t ondisk_size<checkpoint>() noexcept;
template<> disk_offset_t ondisk_size<next_metadata_cluster>() noexcept;
template<> disk_offset_t ondisk_size<create_inode>() noexcept;
template<> disk_offset_t ondisk_size<delete_inode>() noexcept;
template<> disk_offset_t ondisk_size<small_write>(size_t data_len) noexcept;
template<> disk_offset_t ondisk_size<medium_write>() noexcept;
template<> disk_offset_t ondisk_size<large_write>() noexcept;
template<> disk_offset_t ondisk_size<large_write_without_time>() noexcept;
template<> disk_offset_t ondisk_size<truncate>() noexcept;
template<> disk_offset_t ondisk_size<create_dentry>(size_t dentry_name_len) noexcept;
template<> disk_offset_t ondisk_size<create_inode_as_dentry>(size_t dentry_name_len) noexcept;
template<> disk_offset_t ondisk_size<delete_dentry>(size_t dentry_name_len) noexcept;
template<> disk_offset_t ondisk_size<delete_inode_and_dentry>(size_t dentry_name_len) noexcept;

template<typename T>
disk_offset_t ondisk_size(const T& entry) noexcept = delete;

template<> inline disk_offset_t ondisk_size<checkpoint>(const checkpoint& entry) noexcept {
    return ondisk_size<checkpoint>();
}

template<> inline disk_offset_t ondisk_size<next_metadata_cluster>(const next_metadata_cluster& entry) noexcept {
    return ondisk_size<next_metadata_cluster>();
}

template<> inline disk_offset_t ondisk_size<create_inode>(const create_inode& entry) noexcept {
    return ondisk_size<create_inode>();
}

template<> inline disk_offset_t ondisk_size<delete_inode>(const delete_inode& entry) noexcept {
    return ondisk_size<delete_inode>();
}

template<> inline disk_offset_t ondisk_size<small_write>(const small_write& entry) noexcept {
    return ondisk_size<small_write>(entry.data.size());
}

template<> inline disk_offset_t ondisk_size<medium_write>(const medium_write& entry) noexcept {
    return ondisk_size<medium_write>();
}

template<> inline disk_offset_t ondisk_size<large_write>(const large_write& entry) noexcept {
    return ondisk_size<large_write>();
}

template<> inline disk_offset_t ondisk_size<large_write_without_time>(const large_write_without_time& entry) noexcept {
    return ondisk_size<large_write_without_time>();
}

template<> inline disk_offset_t ondisk_size<truncate>(const truncate& entry) noexcept {
    return ondisk_size<truncate>();
}

template<> inline disk_offset_t ondisk_size<create_dentry>(const create_dentry& entry) noexcept {
    return ondisk_size<create_dentry>(entry.name.size());
}

template<> inline disk_offset_t ondisk_size<create_inode_as_dentry>(const create_inode_as_dentry& entry) noexcept {
    return ondisk_size<create_inode_as_dentry>(entry.name.size());
}

template<> inline disk_offset_t ondisk_size<delete_dentry>(const delete_dentry& entry) noexcept {
    return ondisk_size<delete_dentry>(entry.name.size());
}

template<> inline disk_offset_t ondisk_size<delete_inode_and_dentry>(const delete_inode_and_dentry& entry) noexcept {
    return ondisk_size<delete_inode_and_dentry>(entry.dd.name.size());
}

enum read_error {
    NO_MEM,
    TOO_SMALL,
    INVALID_ENTRY_TYPE,
};

template<typename T>
val_or_err<T, read_error> read(std::string_view& buff) noexcept = delete;

template<> val_or_err<checkpoint, read_error> read<checkpoint>(std::string_view& buff) noexcept;
template<> val_or_err<next_metadata_cluster, read_error> read<next_metadata_cluster>(std::string_view& buff) noexcept;
template<> val_or_err<create_inode, read_error> read<create_inode>(std::string_view& buff) noexcept;
template<> val_or_err<delete_inode, read_error> read<delete_inode>(std::string_view& buff) noexcept;
template<> val_or_err<small_write, read_error> read<small_write>(std::string_view& buff) noexcept;
template<> val_or_err<medium_write, read_error> read<medium_write>(std::string_view& buff) noexcept;
template<> val_or_err<large_write, read_error> read<large_write>(std::string_view& buff) noexcept;
template<> val_or_err<large_write_without_time, read_error> read<large_write_without_time>(std::string_view& buff) noexcept;
template<> val_or_err<truncate, read_error> read<truncate>(std::string_view& buff) noexcept;
template<> val_or_err<create_dentry, read_error> read<create_dentry>(std::string_view& buff) noexcept;
template<> val_or_err<create_inode_as_dentry, read_error> read<create_inode_as_dentry>(std::string_view& buff) noexcept;
template<> val_or_err<delete_dentry, read_error> read<delete_dentry>(std::string_view& buff) noexcept;
template<> val_or_err<delete_inode_and_dentry, read_error> read<delete_inode_and_dentry>(std::string_view& buff) noexcept;

using any_entry = std::variant<
        checkpoint,
        next_metadata_cluster,
        create_inode,
        delete_inode,
        small_write,
        medium_write,
        large_write,
        large_write_without_time,
        truncate,
        create_dentry,
        create_inode_as_dentry,
        delete_dentry,
        delete_inode_and_dentry>;

val_or_err<any_entry, read_error> read_any(std::string_view& buff) noexcept;

// Writes @p entry to @p dest, without bound-checking (this task is for the caller)
template<typename T>
void write(const T& entry, void* dest) noexcept = delete;

template<> void write<checkpoint>(const checkpoint& entry, void* dest) noexcept;
template<> void write<next_metadata_cluster>(const next_metadata_cluster& entry, void* dest) noexcept;
template<> void write<create_inode>(const create_inode& entry, void* dest) noexcept;
template<> void write<delete_inode>(const delete_inode& entry, void* dest) noexcept;
template<> void write<small_write>(const small_write& entry, void* dest) noexcept;
template<> void write<medium_write>(const medium_write& entry, void* dest) noexcept;
template<> void write<large_write>(const large_write& entry, void* dest) noexcept;
template<> void write<large_write_without_time>(const large_write_without_time& entry, void* dest) noexcept;
template<> void write<truncate>(const truncate& entry, void* dest) noexcept;
template<> void write<create_dentry>(const create_dentry& entry, void* dest) noexcept;
template<> void write<create_inode_as_dentry>(const create_inode_as_dentry& entry, void* dest) noexcept;
template<> void write<delete_dentry>(const delete_dentry& entry, void* dest) noexcept;
template<> void write<delete_inode_and_dentry>(const delete_inode_and_dentry& entry, void* dest) noexcept;

} // namespace seastar::fs::backend::metadata_log::entries
