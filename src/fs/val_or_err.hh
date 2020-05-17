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

#include <variant>

namespace seastar::fs {

template<class Val, class Err>
class [[nodiscard]] val_or_err {
    std::variant<Val, Err> _storage;

public:
    constexpr val_or_err() = default;

    template<class T, std::enable_if_t<!std::is_same_v<Val, Err> &&
            (std::is_constructible_v<Val, T&&> || std::is_constructible_v<Err, T&&>), int> = 0>
    constexpr val_or_err(T&& value_or_error) : _storage(std::forward<T>(value_or_error)) {}

private:
    template<class... Args>
    constexpr val_or_err(std::in_place_t, Args&&... args) : _storage(std::forward<Args>(args)...) {}

public:
    template<class... Args>
    static constexpr val_or_err value(Args&&... args) {
        return val_or_err(std::in_place, std::in_place_index<0>, std::forward<Args>(args)...);
    }

    template<class... Args>
    static constexpr val_or_err error(Args&&... args) {
        return val_or_err(std::in_place, std::in_place_index<1>, std::forward<Args>(args)...);
    }

    constexpr val_or_err(const val_or_err&) = default;
    constexpr val_or_err(val_or_err&&) = default;
    constexpr val_or_err& operator=(const val_or_err&) = default;
    constexpr val_or_err& operator=(val_or_err&&) = default;

    constexpr explicit operator bool() const noexcept { return _storage.index() == 0; }

    constexpr Val& value() & noexcept { return std::get<0>(_storage); }
    constexpr Val&& value() && noexcept { return std::move(std::get<0>(_storage)); }
    constexpr const Val& value() const& noexcept { return std::get<0>(_storage); }

    constexpr Err& error() & noexcept { return std::get<1>(_storage); }
    constexpr Err&& error() && noexcept { return std::move(std::get<1>(_storage)); }
    constexpr const Err& error() const& noexcept { return std::get<1>(_storage); }
};

} // namespace seastar::fs

#define TRY_VAL_OR_ERR(val_name, val_or_err_name, expr) \
    auto val_or_err_name = expr; \
    if (!val_or_err_name) { \
        return val_or_err_name.error(); \
    } \
    auto& val_name = val_or_err_name.value()

#define TRY_VAL(val_name, expr) TRY_VAL_OR_ERR(val_name, val_name ## __val_or_err, expr)
