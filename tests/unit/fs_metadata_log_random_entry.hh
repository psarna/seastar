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

#include "fs/metadata_log/entries.hh"
#include "fs/metadata_log/to_disk_buffer.hh"
#include "fs_metadata_log_entries_random.hh"
#include "fs_random.hh"

namespace seastar::fs::metadata_log {

using any_entry = std::variant<
        entries::next_metadata_cluster,
        entries::create_inode,
        entries::delete_inode,
        entries::small_write,
        entries::medium_write,
        entries::large_write_without_time,
        entries::large_write,
        entries::truncate,
        entries::create_dentry,
        entries::create_inode_as_dentry,
        entries::delete_dentry,
        entries::delete_inode_and_dentry>;

any_entry random_entry(size_t variable_len_entry_max_len) {
    switch (random_value(0, std::variant_size_v<any_entry> - 1)) {
    case 0: return entries::random_entry<entries::next_metadata_cluster>();
    case 1: return entries::random_entry<entries::create_inode>();
    case 2: return entries::random_entry<entries::delete_inode>();
    case 3: return entries::random_entry<entries::small_write>(variable_len_entry_max_len);
    case 4: return entries::random_entry<entries::medium_write>();
    case 5: return entries::random_entry<entries::large_write_without_time>();
    case 6: return entries::random_entry<entries::large_write>();
    case 7: return entries::random_entry<entries::truncate>();
    case 8: return entries::random_entry<entries::create_dentry>(variable_len_entry_max_len);
    case 9: return entries::random_entry<entries::create_inode_as_dentry>(variable_len_entry_max_len);
    case 10: return entries::random_entry<entries::delete_dentry>(variable_len_entry_max_len);
    case 11: return entries::random_entry<entries::delete_inode_and_dentry>(variable_len_entry_max_len);
    }
    assert(false);
}

auto disk_size(const any_entry& entry) noexcept {
    return std::visit([](auto& e) {
        return entries::ondisk_size(e);
    }, entry);
}

auto append_entry(to_disk_buffer& buf, const any_entry& entry) {
    return std::visit([&buf](auto& e) {
        return buf.append(e);
    }, entry);
}

} // namespace seastar::fs::metadata_log
