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

#include "fs/backend/metadata_log/entries.hh"
#include "fs_unix_metadata_compare.hh"

namespace seastar::fs::backend::metadata_log::entries {

constexpr bool operator==(const checkpoint& a, const checkpoint& b) noexcept {
    return a.crc32_checksum == b.crc32_checksum && a.checkpointed_data_length == b.checkpointed_data_length;
}
constexpr bool operator!=(const checkpoint& a, const checkpoint& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const next_metadata_cluster& a, const next_metadata_cluster& b) noexcept {
    return a.cluster_id == b.cluster_id;
}
constexpr bool operator!=(const next_metadata_cluster& a, const next_metadata_cluster& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const create_inode& a, const create_inode& b) noexcept {
    return a.inode == b.inode && a.metadata == b.metadata;
}
constexpr bool operator!=(const create_inode& a, const create_inode& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const delete_inode& a, const delete_inode& b) noexcept {
    return a.inode == b.inode;
}
constexpr bool operator!=(const delete_inode& a, const delete_inode& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const small_write& a, const small_write& b) noexcept {
    return a.inode == b.inode && a.offset == b.offset && a.time_ns == b.time_ns && a.data == b.data;
}
constexpr bool operator!=(const small_write& a, const small_write& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const medium_write& a, const medium_write& b) noexcept {
    return a.inode == b.inode && a.offset == b.offset && a.drange == b.drange && a.time_ns == b.time_ns;
}
constexpr bool operator!=(const medium_write& a, const medium_write& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const large_write_without_time& a, const large_write_without_time& b) noexcept {
    return a.inode == b.inode && a.offset == b.offset && a.data_cluster == b.data_cluster;
}
constexpr bool operator!=(const large_write_without_time& a, const large_write_without_time& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const large_write& a, const large_write& b) noexcept {
    return a.lwwt == b.lwwt && a.time_ns == b.time_ns;
}
constexpr bool operator!=(const large_write& a, const large_write& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const truncate& a, const truncate& b) noexcept {
    return a.inode == b.inode && a.size == b.size && a.time_ns == b.time_ns;
}
constexpr bool operator!=(const truncate& a, const truncate& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const create_dentry& a, const create_dentry& b) noexcept {
    return a.inode == b.inode && a.name == b.name && a.dir_inode == b.dir_inode;
}
constexpr bool operator!=(const create_dentry& a, const create_dentry& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const create_inode_as_dentry& a, const create_inode_as_dentry& b) noexcept {
    return a.inode == b.inode && a.name == b.name && a.dir_inode == b.dir_inode;
}
constexpr bool operator!=(const create_inode_as_dentry& a, const create_inode_as_dentry& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const delete_dentry& a, const delete_dentry& b) noexcept {
    return a.dir_inode == b.dir_inode && a.name == b.name;
}
constexpr bool operator!=(const delete_dentry& a, const delete_dentry& b) noexcept {
    return !(a == b);
}

constexpr bool operator==(const delete_inode_and_dentry& a, const delete_inode_and_dentry& b) noexcept {
    return a.di == b.di && a.dd == b.dd;
}
constexpr bool operator!=(const delete_inode_and_dentry& a, const delete_inode_and_dentry& b) noexcept {
    return !(a == b);
}

} // seastar::fs::backend::metadata_log::entries
