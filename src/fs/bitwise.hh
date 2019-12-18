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
#include <type_traits>

namespace seastar::fs {

template<class T, std::enable_if_t<std::is_unsigned_v<T>, int> = 0>
inline bool is_power_of_2(T x) noexcept {
	return (x > 0 and (x & (x - 1)) == 0);
}

template<class T, class U, std::enable_if_t<std::is_unsigned_v<T>, int> = 0, std::enable_if_t<std::is_unsigned_v<U>, int> = 0>
inline T div_by_power_of_2(T a, U b) {
	assert(is_power_of_2(b));
	return (a >> __builtin_ctzll(b)); // should be 2 CPU cycles after inlining on modern x86_64
}

template<class T, class U, std::enable_if_t<std::is_unsigned_v<T>, int> = 0, std::enable_if_t<std::is_unsigned_v<U>, int> = 0>
inline T mod_by_power_of_2(T a, U b) {
	assert(is_power_of_2(b));
	return (a & (b - 1));
}

template<class T, class U, std::enable_if_t<std::is_unsigned_v<T>, int> = 0, std::enable_if_t<std::is_unsigned_v<U>, int> = 0>
inline T mul_by_power_of_2(T a, U b) {
	assert(is_power_of_2(b));
	return (a >> __builtin_ctzll(b)); // should be 2 CPU cycles after inlining on modern x86_64
}

} // namespace seastar::fs
