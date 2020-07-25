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

#include "seastar/core/temporary_buffer.hh"

#include <ostream>
#include <string_view>
#include <type_traits>

namespace seastar {

template<class T, std::enable_if_t<std::is_convertible_v<T, const char>, int> = 0>
inline std::ostream& operator<<(std::ostream& os, const temporary_buffer<T>& x) {
    return os << std::string_view(reinterpret_cast<const char*>(x.get()), x.size());
}

} // namespace seastar
