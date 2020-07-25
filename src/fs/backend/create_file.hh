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
#include "fs/backend/shard.hh"
#include "fs/path.hh"
#include "fs/unix_metadata.hh"
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
            while (!path.empty() && path.back() == '/') {
                path.pop_back();
            }
        }

        _entry_name = path::extract_last_component(path);
        if (_entry_name.empty()) {
            return make_exception_future<inode_t>(invalid_path_exception());
        }
        assert(path.empty() || path.back() == '/'); // Hence fast-checking for "is directory" is done in path_lookup

        _perms = perms;
        return _shard.path_lookup(path).then([this](inode_t dir_inode) {
            _dir_inode = dir_inode;
            // Fail-fast checks before locking (as locking may be expensive)
            auto dir_it = _shard._inodes.find(_dir_inode);
            if (dir_it == _shard._inodes.end()) {
                return make_exception_future<inode_t>(operation_became_invalid_exception());
            }
            assert(dir_it->second.is_directory() && "Directory cannot become file or there is a BUG in path_lookup");
            _dir_info = &dir_it->second.get_directory();

            if (_dir_info->entries.count(_entry_name) != 0) {
                return make_exception_future<inode_t>(file_already_exists_exception());
            }

            return _shard._locks.with_locks(shard::locks::shared{dir_inode},
                    shard::locks::unique{dir_inode, _entry_name}, [this] {
                return create_file_in_directory();
            });
        });
    }

    future<inode_t> create_file_in_directory() {
        if (!_shard.inode_exists(_dir_inode)) {
            return make_exception_future<inode_t>(operation_became_invalid_exception());
        }
        if (_dir_info->entries.count(_entry_name) != 0) {
            return make_exception_future<inode_t>(file_already_exists_exception());
        }
        namespace mle = metadata_log::entries;
        if (_entry_name.size() > mle::dentry_name_max_len) {
            return make_exception_future<inode_t>(filename_too_long_exception());
        }
        auto curr_time_ns = _shard._clock->current_time_ns();

        mle::create_inode_as_dentry entry = {
            .inode = {
                .inode = _shard._inode_allocator.alloc(), // TODO: maybe do something if it fails (overflows)
                .metadata = {
                    .ftype = [this] {
                        switch (_create_semantics) {
                        case create_semantics::CREATE_FILE:
                        case create_semantics::CREATE_AND_OPEN_FILE:
                            return file_type::REGULAR_FILE;
                        case create_semantics::CREATE_DIR:
                            return file_type::DIRECTORY;
                        }
                        __builtin_unreachable();
                    }(),
                    .perms = _perms,
                    .uid = 0, // TODO: Eventually, we'll want a user to be able to pass his credentials when bootstrapping the
                    .gid = 0, //       file system -- that will allow us to authorize users on startup (e.g. via LDAP or whatnot).
                    .btime_ns = curr_time_ns,
                    .mtime_ns = curr_time_ns,
                    .ctime_ns = curr_time_ns,
                },
            },
            .name = std::move(_entry_name),
            .dir_inode = _dir_inode,
        };

        // TODO: add check that the culster_size is not too small as it would cause to allocate all clusters
        //       and then return error ENOSPACE
        switch (_shard.append_metadata_log(entry)) {
        case shard::append_result::TOO_BIG:
            return make_exception_future<inode_t>(cluster_size_too_small_to_perform_operation_exception());
        case shard::append_result::NO_SPACE:
            return make_exception_future<inode_t>(no_more_space_exception());
        case shard::append_result::APPENDED:
            inode_info& new_inode_info = _shard.memory_only_create_inode(entry.inode.inode, entry.inode.metadata);
            _shard.memory_only_create_dentry(*_dir_info, entry.inode.inode, std::move(entry.name));

            switch (_create_semantics) {
            case create_semantics::CREATE_FILE:
            case create_semantics::CREATE_DIR:
                break;
            case create_semantics::CREATE_AND_OPEN_FILE:
                // We don't have to lock, as there was no context switch since the allocation of the inode number
                ++new_inode_info.opened_files_count;
                break;
            }

            return make_ready_future<inode_t>(entry.inode.inode);
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
