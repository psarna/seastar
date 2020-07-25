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
#include "fs_range_print.hh"
#include "fs_unix_metadata_print.hh"
#include "seastar/fs/overloaded.hh"
#include "temporary_buffer_print.hh"

#include <ostream>
#include <variant>

namespace seastar::fs::backend::metadata_log::entries {

inline std::ostream& operator<<(std::ostream& os, const checkpoint& x) {
    auto flags = os.flags();
    os << "{crc32_checksum=" << std::ios::hex << x.crc32_checksum;
    os.flags(flags);
    os << ",checkpointed_data_length=" << x.checkpointed_data_length;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const next_metadata_cluster& x) {
    os << "{cluster_id=" << x.cluster_id;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const create_inode& x) {
    os << "{inode=" << x.inode;
    os << ",metadata=" << x.metadata;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const delete_inode& x) {
    os << "{inode=" << x.inode;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const small_write& x) {
    os << "{inode=" << x.inode;
    os << ",offset=" << x.offset;
    os << ",time_ns=" << x.time_ns;
    os << ",data.size()=" << x.data.size();
    os << ",data=" << x.data;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const medium_write& x) {
    os << "{inode=" << x.inode;
    os << ",offset=" << x.offset;
    os << ",drange=" << x.drange;
    os << ",time_ns=" << x.time_ns;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const large_write_without_time& x) {
    os << "{inode=" << x.inode;
    os << ",offset=" << x.offset;
    os << ",data_cluster=" << x.data_cluster;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const large_write& x) {
    os << "{lwwt=" << x.lwwt;
    os << ",time_ns=" << x.time_ns;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const truncate& x) {
    os << "{inode=" << x.inode;
    os << ",size=" << x.size;
    os << ",time_ns=" << x.time_ns;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const create_dentry& x) {
    os << "{inode=" << x.inode;
    os << ",name=\'" << x.name << '\'';
    os << ",dir_inode=" << x.dir_inode;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const create_inode_as_dentry& x) {
    os << "{inode=" << x.inode;
    os << ",name=\'" << x.name << '\'';
    os << ",dir_inode=" << x.dir_inode;
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const delete_dentry& x) {
    os << "{dir_inode=" << x.dir_inode;
    os << ",name=\'" << x.name << '\'';
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const delete_inode_and_dentry& x) {
    os << "{di=" << x.di;
    os << ",dd=\'" << x.dd << '\'';
    return os << '}';
}

inline std::ostream& operator<<(std::ostream& os, const any_entry& x) {
    std::visit(overloaded{
        [&os](const checkpoint& y) { os << "checkpoint:" << y; },
        [&os](const next_metadata_cluster& y) { os << "next_metadata_cluster:" << y; },
        [&os](const create_inode& y) { os << "create_inode:" << y; },
        [&os](const delete_inode& y) { os << "delete_inode:" << y; },
        [&os](const small_write& y) { os << "small_write:" << y; },
        [&os](const medium_write& y) { os << "medium_write:" << y; },
        [&os](const large_write& y) { os << "large_write:" << y; },
        [&os](const large_write_without_time& y) { os << "large_write_without_time:" << y; },
        [&os](const truncate& y) { os << "truncate:" << y; },
        [&os](const create_dentry& y) { os << "create_dentry:" << y; },
        [&os](const create_inode_as_dentry& y) { os << "create_inode_as_dentry:" << y; },
        [&os](const delete_dentry& y) { os << "delete_dentry:" << y; },
        [&os](const delete_inode_and_dentry& y) { os << "delete_inode_and_dentry:" << y; },
    }, x);
    return os;
}

} // namespace seastar::fs::backend::metadata_log::entries
