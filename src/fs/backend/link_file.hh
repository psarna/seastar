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

#include "fs/backend/shard.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/path.hh"

namespace seastar::fs::backend {

class link_file_operation {
    shard& _shard;
    inode_t _src_inode;
    std::string _entry_name;
    inode_t _dir_inode;
    inode_info::directory* _dir_info;

    link_file_operation(shard& shard) : _shard(shard) {}

    future<> link_file(inode_t inode, std::string path) {
        _src_inode = inode;
        _entry_name = extract_last_component(path);
        if (_entry_name.empty()) {
            return make_exception_future(is_directory_exception());
        }
        assert(path.empty() or path.back() == '/'); // Hence fast-checking for "is directory" is done in path_lookup

        return _shard.path_lookup(path).then([this](inode_t dir_inode) {
            _dir_inode = dir_inode;
            // Fail-fast checks before locking (as locking may be expensive)
            auto dir_it = _shard._inodes.find(_dir_inode);
            if (dir_it == _shard._inodes.end()) {
                return make_exception_future(operation_became_invalid_exception());
            }
            assert(dir_it->second.is_directory() and "Directory cannot become file or there is a BUG in path_lookup");
            _dir_info = &dir_it->second.get_directory();

            if (_dir_info->entries.count(_entry_name) != 0) {
                return make_exception_future(file_already_exists_exception());
            }

            return _shard._locks.with_locks(shard::locks::shared {dir_inode},
                    shard::locks::unique {dir_inode, _entry_name}, [this] {
                return link_file_in_directory();
            });
        });
    }

    future<> link_file_in_directory() {
        if (not _shard.inode_exists(_dir_inode)) {
            return make_exception_future(operation_became_invalid_exception());
        }

        if (_dir_info->entries.count(_entry_name) != 0) {
            return make_exception_future(file_already_exists_exception());
        }

        ondisk_add_dir_entry_header ondisk_entry;
        decltype(ondisk_entry.entry_name_length) entry_name_length;
        if (_entry_name.size() > std::numeric_limits<decltype(entry_name_length)>::max()) {
            // TODO: add an assert that the culster_size is not too small as it would cause to allocate all clusters
            //       and then return error ENOSPACE
            return make_exception_future(filename_too_long_exception());
        }
        entry_name_length = _entry_name.size();

        ondisk_entry = {
            _dir_inode,
            _src_inode,
            entry_name_length,
        };

        switch (_shard.append_ondisk_entry(ondisk_entry, _entry_name.data())) {
        case shard::append_result::TOO_BIG:
            return make_exception_future(cluster_size_too_small_to_perform_operation_exception());
        case shard::append_result::NO_SPACE:
            return make_exception_future(no_more_space_exception());
        case shard::append_result::APPENDED:
            _shard.memory_only_add_dir_entry(*_dir_info, _src_inode, std::move(_entry_name));
            return now();
        }
        __builtin_unreachable();
    }

public:
    static future<> perform(shard& shard, inode_t inode, std::string path) {
        return do_with(link_file_operation(shard), [inode, path = std::move(path)](auto& obj) {
            return obj.link_file(inode, std::move(path));
        });
    }
};

} // namespace seastar::fs::backend
