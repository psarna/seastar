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

#pragma once

#include "fs/cluster.hh"
#include "fs/inode.hh"
#include "fs/unix_metadata.hh"

namespace seastar::fs {

struct ondisk_unix_metadata {
    uint32_t perms;
    uint32_t uid;
    uint32_t gid;
    uint64_t btime_ns;
    uint64_t mtime_ns;
    uint64_t ctime_ns;
} __attribute__((packed));

static_assert(sizeof(decltype(ondisk_unix_metadata::perms)) >= sizeof(decltype(unix_metadata::perms)));
static_assert(sizeof(decltype(ondisk_unix_metadata::uid)) >= sizeof(decltype(unix_metadata::uid)));
static_assert(sizeof(decltype(ondisk_unix_metadata::gid)) >= sizeof(decltype(unix_metadata::gid)));
static_assert(sizeof(decltype(ondisk_unix_metadata::btime_ns)) >= sizeof(decltype(unix_metadata::btime_ns)));
static_assert(sizeof(decltype(ondisk_unix_metadata::mtime_ns)) >= sizeof(decltype(unix_metadata::mtime_ns)));
static_assert(sizeof(decltype(ondisk_unix_metadata::ctime_ns)) >= sizeof(decltype(unix_metadata::ctime_ns)));

inline unix_metadata ondisk_metadata_to_metadata(const ondisk_unix_metadata& ondisk_metadata) noexcept {
    unix_metadata res;
    static_assert(sizeof(ondisk_metadata) == 36,
            "metadata size changed: check if above static asserts and below assignments need update");
    res.perms = static_cast<file_permissions>(ondisk_metadata.perms);
    res.uid = ondisk_metadata.uid;
    res.gid = ondisk_metadata.gid;
    res.btime_ns = ondisk_metadata.btime_ns;
    res.mtime_ns = ondisk_metadata.mtime_ns;
    res.ctime_ns = ondisk_metadata.ctime_ns;
    return res;
}

inline ondisk_unix_metadata metadata_to_ondisk_metadata(const unix_metadata& metadata) noexcept {
    ondisk_unix_metadata res;
    static_assert(sizeof(res) == 36, "metadata size changed: check if below assignments need update");
    res.perms = static_cast<decltype(res.perms)>(metadata.perms);
    res.uid = metadata.uid;
    res.gid = metadata.gid;
    res.btime_ns = metadata.btime_ns;
    res.mtime_ns = metadata.mtime_ns;
    res.ctime_ns = metadata.ctime_ns;
    return res;
}

enum ondisk_type : uint8_t {
    INVALID = 0,
    CHECKPOINT,
    NEXT_METADATA_CLUSTER,
    CREATE_INODE,
    DELETE_INODE,
    SMALL_WRITE,
    MEDIUM_WRITE,
    LARGE_WRITE,
    LARGE_WRITE_WITHOUT_MTIME,
    TRUNCATE,
    ADD_DIR_ENTRY,
    CREATE_INODE_AS_DIR_ENTRY,
    DELETE_DIR_ENTRY,
    DELETE_INODE_AND_DIR_ENTRY,
};

struct ondisk_checkpoint {
    // The disk format is as follows:
    // | ondisk_checkpoint | .............................. |
    //                     |             data               |
    //                     |<-- checkpointed_data_length -->|
    //                                                      ^
    //       ______________________________________________/
    //      /
    //    there ends checkpointed data and (next checkpoint begins or metadata in the current cluster end)
    //
    // CRC is calculated from byte sequence | data | checkpointed_data_length |
    // E.g. if the data consist of bytes "abcd" and checkpointed_data_length of bytes "xyz" then the byte sequence
    // would be "abcdxyz"
    uint32_t crc32_code;
    unit_size_t checkpointed_data_length;
} __attribute__((packed));

struct ondisk_next_metadata_cluster {
    cluster_id_t cluster_id; // metadata log continues there
} __attribute__((packed));

struct ondisk_create_inode {
    inode_t inode;
    uint8_t is_directory;
    ondisk_unix_metadata metadata;
} __attribute__((packed));

struct ondisk_delete_inode {
    inode_t inode;
} __attribute__((packed));

struct ondisk_small_write_header {
    inode_t inode;
    file_offset_t offset;
    uint16_t length;
    decltype(unix_metadata::mtime_ns) time_ns;
    // After header comes data
} __attribute__((packed));

struct ondisk_medium_write {
    inode_t inode;
    file_offset_t offset;
    disk_offset_t disk_offset;
    uint32_t length;
    decltype(unix_metadata::mtime_ns) time_ns;
} __attribute__((packed));

struct ondisk_large_write {
    inode_t inode;
    file_offset_t offset;
    cluster_id_t data_cluster; // length == cluster_size
    decltype(unix_metadata::mtime_ns) time_ns;
} __attribute__((packed));

struct ondisk_large_write_without_mtime {
    inode_t inode;
    file_offset_t offset;
    cluster_id_t data_cluster; // length == cluster_size
} __attribute__((packed));

struct ondisk_truncate {
    inode_t inode;
    file_offset_t size;
    decltype(unix_metadata::mtime_ns) time_ns;
} __attribute__((packed));

struct ondisk_add_dir_entry_header {
    inode_t dir_inode;
    inode_t entry_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));

struct ondisk_create_inode_as_dir_entry_header {
    ondisk_create_inode entry_inode;
    inode_t dir_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));

struct ondisk_delete_dir_entry_header {
    inode_t dir_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));

struct ondisk_delete_inode_and_dir_entry_header {
    inode_t inode_to_delete;
    inode_t dir_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));

template<typename T>
constexpr size_t ondisk_entry_size(const T& entry) noexcept {
    static_assert(std::is_same_v<T, ondisk_next_metadata_cluster> or
            std::is_same_v<T, ondisk_create_inode> or
            std::is_same_v<T, ondisk_delete_inode> or
            std::is_same_v<T, ondisk_medium_write> or
            std::is_same_v<T, ondisk_large_write> or
            std::is_same_v<T, ondisk_large_write_without_mtime> or
            std::is_same_v<T, ondisk_truncate>, "ondisk entry size not defined for given type");
    return sizeof(ondisk_type) + sizeof(entry);
}
constexpr size_t ondisk_entry_size(const ondisk_small_write_header& entry) noexcept {
    return sizeof(ondisk_type) + sizeof(entry) + entry.length;
}
constexpr size_t ondisk_entry_size(const ondisk_add_dir_entry_header& entry) noexcept {
    return sizeof(ondisk_type) + sizeof(entry) + entry.entry_name_length;
}
constexpr size_t ondisk_entry_size(const ondisk_create_inode_as_dir_entry_header& entry) noexcept {
    return sizeof(ondisk_type) + sizeof(entry) + entry.entry_name_length;
}
constexpr size_t ondisk_entry_size(const ondisk_delete_dir_entry_header& entry) noexcept {
    return sizeof(ondisk_type) + sizeof(entry) + entry.entry_name_length;
}
constexpr size_t ondisk_entry_size(const ondisk_delete_inode_and_dir_entry_header& entry) noexcept {
    return sizeof(ondisk_type) + sizeof(entry) + entry.entry_name_length;
}

} // namespace seastar::fs
