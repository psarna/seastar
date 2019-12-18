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

#include "bitwise.hh"

#include <cstdint>

namespace seastar::fs {

// Inode has bitwise format: `in_shard_inode | shard_no` where a constant number of bits are reserved for shard_no
using inode_t = uint64_t;

inline unsigned inode_to_shard_no(inode_t inode, unsigned shard_pool_size) noexcept {
	assert(is_power_of_2(shard_pool_size));
	return mod_by_power_of_2(inode, shard_pool_size);
}

inline unsigned next_shard_inode(inode_t last_shard_inode, unsigned shard_pool_size) noexcept {
	return last_shard_inode + shard_pool_size;
}

} // namespace seastar::fs
