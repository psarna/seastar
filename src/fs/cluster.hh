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

#include <cassert>
#include <cstdint>

namespace seastar::fs {

using offset_t = uint64_t;
using cluster_id_t = uint64_t;
using unit_size_t = uint32_t;

struct cluster_range {
	cluster_id_t beg;
	cluster_id_t end; // exclusive
};

inline bool operator==(cluster_range a, cluster_range b) noexcept {
	return (a.beg == b.beg and a.end == b.end);
}

inline bool operator!=(cluster_range a, cluster_range b) noexcept {
	return not (a == b);
}

inline bool is_power_of_2(uint32_t x) noexcept {
	return (x > 0 and (x & (x - 1)) == 0);
}

inline cluster_id_t offset_to_cluster_id(offset_t offset, unit_size_t cluster_size) noexcept {
	assert(is_power_of_2(cluster_size));
	return (offset >> __builtin_ctzll(cluster_size)); // should be 2 CPU cycles after inlining on modern x86_64
}

inline offset_t cluster_id_to_offset(cluster_id_t cluster_id, unit_size_t cluster_size) noexcept {
	assert(is_power_of_2(cluster_size));
	return cluster_id * cluster_size; // should be 2 CPU cycles after inlining on modern x86_64
}

} // namespace seastar::fs
