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

#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "fs/units.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/fs/exceptions.hh"

namespace seastar::fs {

class truncate_operation {

    metadata_log& _metadata_log;
    inode_t _inode;

    truncate_operation(metadata_log& metadata_log, inode_t inode)
        : _metadata_log(metadata_log), _inode(inode) {
    }

    future<> truncate(file_offset_t size) {
        auto inode_it = _metadata_log._inodes.find(_inode);
        if (inode_it == _metadata_log._inodes.end()) {
            return make_exception_future(invalid_inode_exception());
        }
        if (inode_it->second.is_directory()) {
            return make_exception_future(is_directory_exception());
        }

        return _metadata_log._locks.with_lock(metadata_log::locks::shared {_inode}, [this, size] {
            if (not _metadata_log.inode_exists(_inode)) {
                return make_exception_future(operation_became_invalid_exception());
            }
            return do_truncate(size);
        });
    }

    future<> do_truncate(file_offset_t size) {
        if (_metadata_log._read_only) {
            return make_exception_future(read_only_filesystem_exception());
        }

        using namespace std::chrono;
        uint64_t curr_time_ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
        ondisk_truncate ondisk_entry {
            _inode,
            size,
            curr_time_ns
        };

        switch (_metadata_log.append_ondisk_entry(ondisk_entry)) {
        case metadata_log::append_result::TOO_BIG:
            return make_exception_future(cluster_size_too_small_to_perform_operation_exception());
        case metadata_log::append_result::NO_SPACE:
            return make_exception_future(no_more_space_exception());
        case metadata_log::append_result::APPENDED:
            _metadata_log.memory_only_truncate(_inode, size);
            _metadata_log.memory_only_update_mtime(_inode, curr_time_ns);
            return make_ready_future();
        }
        __builtin_unreachable();
    }

public:
    static future<> perform(metadata_log& metadata_log, inode_t inode, file_offset_t size) {
        return do_with(truncate_operation(metadata_log, inode), [size](auto& obj) {
            return obj.truncate(size);
        });
    }
};

} // namespace seastar::fs
