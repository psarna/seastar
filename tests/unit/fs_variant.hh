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

#include <type_traits>
#include <variant>

namespace seastar::fs {

namespace detail {

template<class, class... Ts>
struct extend_variant {};

template<class... Vtp, class... Ts>
struct extend_variant<std::variant<Vtp...>, Ts...> {
    using type = std::variant<Vtp..., Ts...>;
};

} // namespace detail

template<class Variant, class... Ts>
using extend_variant = typename detail::extend_variant<Variant, Ts...>::type;

template<class T, class... Options>
constexpr bool is_one_of = (std::is_same_v<T, Options> || ...);

template<class, class...>
constexpr bool has_alternative = false;

template<class T, class... Vtp>
constexpr bool has_alternative<T, std::variant<Vtp...>> = is_one_of<T, Vtp...>;

} // seastar::fs
