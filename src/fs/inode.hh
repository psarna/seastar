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

#include "fs/bitwise.hh"
#include "fs/units.hh"

#include <cstdint>
#include <optional>

namespace seastar::fs {

// Last log2(fs_shards_pool_size bits) of the inode number contain the id of shard that owns the inode
using inode_t = uint64_t;

// Obtains shard id of the shard owning @p inode.
//@p fs_shards_pool_size is the number of file system shards rounded up to a power of 2
inline fs_shard_id_t inode_to_shard_no(inode_t inode, fs_shard_id_t fs_shards_pool_size) noexcept {
    assert(is_power_of_2(fs_shards_pool_size));
    return mod_by_power_of_2(inode, fs_shards_pool_size);
}

// Returns inode belonging to the shard owning @p shard_previous_inode that is next after @p shard_previous_inode
// (i.e. the lowest inode greater than @p shard_previous_inode belonging to the same shard)
//@p fs_shards_pool_size is the number of file system shards rounded up to a power of 2
inline inode_t shard_next_inode(inode_t shard_previous_inode, fs_shard_id_t fs_shards_pool_size) noexcept {
    return shard_previous_inode + fs_shards_pool_size;
}

// Returns first inode (lowest by value) belonging to the shard @p fs_shard_id
inline inode_t shard_first_inode(fs_shard_id_t fs_shard_id) noexcept {
    return fs_shard_id;
}

class shard_inode_allocator {
    fs_shard_id_t _fs_shards_pool_size;
    fs_shard_id_t _fs_shard_id;
    std::optional<inode_t> _latest_allocated_inode;

public:
    shard_inode_allocator(fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id, std::optional<inode_t> latest_allocated_inode = std::nullopt)
    : _fs_shards_pool_size(fs_shards_pool_size)
    , _fs_shard_id(fs_shard_id)
    , _latest_allocated_inode(latest_allocated_inode) {}

    inode_t alloc() noexcept {
        if (not _latest_allocated_inode) {
            _latest_allocated_inode = shard_first_inode(_fs_shard_id);
            return *_latest_allocated_inode;
        }

        _latest_allocated_inode = shard_next_inode(*_latest_allocated_inode, _fs_shards_pool_size);
        return *_latest_allocated_inode;
    }

    void reset(std::optional<inode_t> latest_allocated_inode = std::nullopt) noexcept {
        _latest_allocated_inode = latest_allocated_inode;
    }
};

} // namespace seastar::fs
