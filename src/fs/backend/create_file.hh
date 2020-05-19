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
#include "fs/path.hh"
#include "seastar/core/future.hh"

namespace seastar::fs::backend {

enum class create_semantics {
    CREATE_FILE,
    CREATE_AND_OPEN_FILE,
    CREATE_DIR,
};

class create_file_operation {
    shard& _shard;
    create_semantics _create_semantics;
    std::string _entry_name;
    file_permissions _perms;
    inode_t _dir_inode;
    inode_info::directory* _dir_info;

    create_file_operation(shard& shard) : _shard(shard) {}

    future<inode_t> create_file(std::string path, file_permissions perms, create_semantics create_semantics) {
        _create_semantics = create_semantics;
        switch (create_semantics) {
        case create_semantics::CREATE_FILE:
        case create_semantics::CREATE_AND_OPEN_FILE:
            break;
        case create_semantics::CREATE_DIR:
            while (not path.empty() and path.back() == '/') {
                path.pop_back();
            }
        }

        _entry_name = extract_last_component(path);
        if (_entry_name.empty()) {
            return make_exception_future<inode_t>(invalid_path_exception());
        }
        assert(path.empty() or path.back() == '/'); // Hence fast-checking for "is directory" is done in path_lookup

        _perms = perms;
        return _shard.path_lookup(path).then([this](inode_t dir_inode) {
            _dir_inode = dir_inode;
            // Fail-fast checks before locking (as locking may be expensive)
            auto dir_it = _shard._inodes.find(_dir_inode);
            if (dir_it == _shard._inodes.end()) {
                return make_exception_future<inode_t>(operation_became_invalid_exception());
            }
            assert(dir_it->second.is_directory() and "Directory cannot become file or there is a BUG in path_lookup");
            _dir_info = &dir_it->second.get_directory();

            if (_dir_info->entries.count(_entry_name) != 0) {
                return make_exception_future<inode_t>(file_already_exists_exception());
            }

            return _shard._locks.with_locks(shard::locks::shared {dir_inode},
                    shard::locks::unique {dir_inode, _entry_name}, [this] {
                return create_file_in_directory();
            });
        });
    }

    future<inode_t> create_file_in_directory() {
        if (not _shard.inode_exists(_dir_inode)) {
            return make_exception_future<inode_t>(operation_became_invalid_exception());
        }

        if (_dir_info->entries.count(_entry_name) != 0) {
            return make_exception_future<inode_t>(file_already_exists_exception());
        }

        ondisk_create_inode_as_dir_entry_header ondisk_entry;
        decltype(ondisk_entry.entry_name_length) entry_name_length;
        if (_entry_name.size() > std::numeric_limits<decltype(entry_name_length)>::max()) {
            // TODO: add an assert that the culster_size is not too small as it would cause to allocate all clusters
            //       and then return error ENOSPACE
            return make_exception_future<inode_t>(filename_too_long_exception());
        }
        entry_name_length = _entry_name.size();

        using namespace std::chrono;
        uint64_t curr_time_ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
        unix_metadata unx_mtdt = {
            _perms,
            0, // TODO: Eventually, we'll want a user to be able to pass his credentials when bootstrapping the
            0, //       file system -- that will allow us to authorize users on startup (e.g. via LDAP or whatnot).
            curr_time_ns,
            curr_time_ns,
            curr_time_ns
        };

        bool creating_dir = [this] {
            switch (_create_semantics) {
            case create_semantics::CREATE_FILE:
            case create_semantics::CREATE_AND_OPEN_FILE:
                return false;
            case create_semantics::CREATE_DIR:
                return true;
            }
            __builtin_unreachable();
        }();

        inode_t new_inode = _shard._inode_allocator.alloc();

        ondisk_entry = {
            {
                new_inode,
                creating_dir,
                metadata_to_ondisk_metadata(unx_mtdt)
            },
            _dir_inode,
            entry_name_length,
        };


        switch (_shard.append_ondisk_entry(ondisk_entry, _entry_name.data())) {
        case shard::append_result::TOO_BIG:
            return make_exception_future<inode_t>(cluster_size_too_small_to_perform_operation_exception());
        case shard::append_result::NO_SPACE:
            return make_exception_future<inode_t>(no_more_space_exception());
        case shard::append_result::APPENDED:
            inode_info& new_inode_info = _shard.memory_only_create_inode(new_inode,
                creating_dir, unx_mtdt);
            _shard.memory_only_add_dir_entry(*_dir_info, new_inode, std::move(_entry_name));

            switch (_create_semantics) {
            case create_semantics::CREATE_FILE:
            case create_semantics::CREATE_DIR:
                break;
            case create_semantics::CREATE_AND_OPEN_FILE:
                // We don't have to lock, as there was no context switch since the allocation of the inode number
                ++new_inode_info.opened_files_count;
                break;
            }

            return make_ready_future<inode_t>(new_inode);
        }
        __builtin_unreachable();
    }

public:
    static future<inode_t> perform(shard& shard, std::string path, file_permissions perms,
            create_semantics create_semantics) {
        return do_with(create_file_operation(shard),
                [path = std::move(path), perms = std::move(perms), create_semantics](auto& obj) {
            return obj.create_file(std::move(path), std::move(perms), create_semantics);
        });
    }
};

} // namespace seastar::fs::backend
