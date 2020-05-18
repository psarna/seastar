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
#include "fs/units.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"

namespace seastar::fs::backend {

class truncate_operation {

    shard& _shard;
    inode_t _inode;

    truncate_operation(shard& shard, inode_t inode)
        : _shard(shard), _inode(inode) {
    }

    future<> truncate(file_offset_t size) {
        auto inode_it = _shard._inodes.find(_inode);
        if (inode_it == _shard._inodes.end()) {
            return make_exception_future(invalid_inode_exception());
        }
        if (inode_it->second.is_directory()) {
            return make_exception_future(is_directory_exception());
        }

        return _shard._locks.with_lock(shard::locks::shared {_inode}, [this, size] {
            if (not _shard.inode_exists(_inode)) {
                return make_exception_future(operation_became_invalid_exception());
            }
            return do_truncate(size);
        });
    }

    future<> do_truncate(file_offset_t size) {
        auto curr_time_ns = _shard._clock->current_time_ns();
        metadata_log::entries::truncate entry = {
            .inode = _inode,
            .size = size,
            .time_ns = curr_time_ns,
        };

        switch (_shard.append_metadata_log(entry)) {
        case shard::append_result::TOO_BIG:
            return make_exception_future(cluster_size_too_small_to_perform_operation_exception());
        case shard::append_result::NO_SPACE:
            return make_exception_future(no_more_space_exception());
        case shard::append_result::APPENDED:
            _shard.memory_only_truncate(_inode, size);
            _shard.memory_only_update_mtime(_inode, curr_time_ns);
            return make_ready_future();
        }
        __builtin_unreachable();
    }

public:
    static future<> perform(shard& shard, inode_t inode, file_offset_t size) {
        return do_with(truncate_operation(shard, inode), [size](auto& obj) {
            return obj.truncate(size);
        });
    }
};

} // namespace seastar::fs::backend
