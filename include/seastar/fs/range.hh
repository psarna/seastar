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

#include <algorithm>

namespace seastar::fs {

template <class T>
struct range {
    T beg;
    T end; // exclusive

    constexpr bool is_empty() const noexcept { return beg >= end; }

    constexpr T size() const noexcept { return end - beg; }
};

template <class T>
range(T beg, T end) -> range<T>;

template <class T>
inline bool operator==(range<T> a, range<T> b) noexcept {
    return (a.beg == b.beg and a.end == b.end);
}

template <class T>
inline bool operator!=(range<T> a, range<T> b) noexcept {
    return not (a == b);
}

template <class T>
inline range<T> intersection(range<T> a, range<T> b) noexcept {
    return {std::max(a.beg, b.beg), std::min(a.end, b.end)};
}

template <class T>
inline bool are_intersecting(range<T> a, range<T> b) noexcept {
    return (std::max(a.beg, b.beg) < std::min(a.end, b.end));
}

} // namespace seastar::fs
