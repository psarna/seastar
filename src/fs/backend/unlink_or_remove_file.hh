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
#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_log/entries.hh"
#include "fs/path.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"

namespace seastar::fs::backend {

enum class remove_semantics {
    FILE_ONLY,
    DIR_ONLY,
    FILE_OR_DIR,
};

class unlink_or_remove_file_operation {
    shard& _shard;
    remove_semantics _remove_semantics;
    std::string _entry_name;
    inode_t _dir_inode;
    inode_info::directory* _dir_info;
    inode_t _entry_inode;
    inode_info* _entry_inode_info;

    unlink_or_remove_file_operation(shard& shard) : _shard(shard) {}

    future<> unlink_or_remove(std::string path, remove_semantics remove_semantics) {
        _remove_semantics = remove_semantics;
        while (not path.empty() and path.back() == '/') {
            path.pop_back();
        }

        _entry_name = extract_last_component(path);
        if (_entry_name.empty()) {
            return make_exception_future(invalid_path_exception()); // We cannot remove "/"
        }
        assert(path.empty() or path.back() == '/'); // Hence fast-check for "is directory" is done in path_lookup

        return _shard.path_lookup(path).then([this](inode_t dir_inode) {
            _dir_inode = dir_inode;
            // Fail-fast checks before locking (as locking may be expensive)
            auto dir_it = _shard._inodes.find(dir_inode);
            if (dir_it == _shard._inodes.end()) {
                return make_exception_future(operation_became_invalid_exception());
            }
            assert(dir_it->second.is_directory() and "Directory cannot become file or there is a BUG in path_lookup");
            _dir_info = &dir_it->second.get_directory();

            auto entry_it = _dir_info->entries.find(_entry_name);
            if (entry_it == _dir_info->entries.end()) {
                return make_exception_future(no_such_file_or_directory_exception());
            }
            _entry_inode = entry_it->second;

            _entry_inode_info = &_shard._inodes.at(_entry_inode);
            if (_entry_inode_info->is_directory()) {
                switch (_remove_semantics) {
                case remove_semantics::FILE_ONLY:
                    return make_exception_future(is_directory_exception());
                case remove_semantics::DIR_ONLY:
                case remove_semantics::FILE_OR_DIR:
                    break;
                }

                if (not _entry_inode_info->get_directory().entries.empty()) {
                    return make_exception_future(directory_not_empty_exception());
                }
            } else {
                assert(_entry_inode_info->is_file());
                switch (_remove_semantics) {
                case remove_semantics::DIR_ONLY:
                    return make_exception_future(is_directory_exception());
                case remove_semantics::FILE_ONLY:
                case remove_semantics::FILE_OR_DIR:
                    break;
                }
            }

            // Getting a lock on directory entry is enough to ensure it won't disappear because deleting directory
            // requires it to be empty
            if (_entry_inode_info->is_directory()) {
                return _shard._locks.with_locks(shard::locks::unique {dir_inode, _entry_name}, shard::locks::unique {_entry_inode}, [this] {
                    return unlink_or_remove_file_in_directory();
                });
            } else {
                return _shard._locks.with_locks(shard::locks::unique {dir_inode, _entry_name}, shard::locks::shared {_entry_inode}, [this] {
                    return unlink_or_remove_file_in_directory();
                });
            }
        });
    }

    future<> unlink_or_remove_file_in_directory() {
        if (not _shard.inode_exists(_dir_inode)) {
            return make_exception_future(operation_became_invalid_exception());
        }
        auto entry_it = _dir_info->entries.find(_entry_name);
        if (entry_it == _dir_info->entries.end() or entry_it->second != _entry_inode) {
            return make_exception_future(operation_became_invalid_exception());
        }

        namespace mle = metadata_log::entries;
        if (_entry_inode_info->is_directory()) {
            inode_info::directory& dir = _entry_inode_info->get_directory();
            if (not dir.entries.empty()) {
                return make_exception_future(directory_not_empty_exception());
            }

            assert(_entry_inode_info->links_count == 1);

            // Ready to delete directory
            assert(_entry_name.size() < mle::dentry_name_max_len);
            mle::delete_inode_and_dentry entry = {
                .di = {.inode = _entry_inode},
                .dd = {
                    .dir_inode = _dir_inode,
                    .name = std::move(_entry_name),
                }
            };

            switch (_shard.append_metadata_log(entry)) {
            case shard::append_result::TOO_BIG:
                return make_exception_future(cluster_size_too_small_to_perform_operation_exception());
            case shard::append_result::NO_SPACE:
                return make_exception_future(no_more_space_exception());
            case shard::append_result::APPENDED:
                _shard.memory_only_delete_dentry(*_dir_info, entry.dd.name);
                _shard.memory_only_delete_inode(entry.di.inode);
                return now();
            }
            __builtin_unreachable();
        }

        assert(_entry_inode_info->is_file());

        // Ready to unlink file
        assert(_entry_name.size() < mle::dentry_name_max_len);
        mle::delete_dentry entry = {
            .dir_inode = _dir_inode,
            .name = std::move(_entry_name),
        };

        switch (_shard.append_metadata_log(entry)) {
        case shard::append_result::TOO_BIG:
            return make_exception_future(cluster_size_too_small_to_perform_operation_exception());
        case shard::append_result::NO_SPACE:
            return make_exception_future(no_more_space_exception());
        case shard::append_result::APPENDED:
            _shard.memory_only_delete_dentry(*_dir_info, entry.name);
            break;
        }

        if (not _entry_inode_info->is_linked() and not _entry_inode_info->is_open()) {
            // File became unlinked and not open, so we need to delete it
            _shard.schedule_attempt_to_delete_inode(_entry_inode);
        }

        return now();
    }

public:
    static future<> perform(shard& shard, std::string path, remove_semantics remove_semantics) {
        return do_with(unlink_or_remove_file_operation(shard), [path = std::move(path), remove_semantics](auto& obj) {
            return obj.unlink_or_remove(std::move(path), remove_semantics);
        });
    }
};

} // namespace seastar::fs::backend
