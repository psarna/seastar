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

#include "seastar/core/shared_mutex.hh"

#include <map>

namespace seastar::fs {

template<class Value>
class value_shared_lock {
    struct lock_info {
        size_t users_num = 0;
        shared_mutex lock;
    };

    std::map<Value, lock_info> _locks;

public:
    value_shared_lock() = default;

    template<class Func>
    auto with_shared_on(const Value& val, Func&& func) {
        auto it = _locks.emplace(val, lock_info {}).first;
        ++it->second.users_num;
        return with_shared(it->second.lock, std::forward<Func>(func)).finally([this, it] {
            if (--it->second.users_num == 0) {
                _locks.erase(it);
            }
        });
    }

    template<class Func>
    auto with_lock_on(const Value& val, Func&& func) {
        auto it = _locks.emplace(val, lock_info {}).first;
        ++it->second.users_num;
        return with_lock(it->second.lock, std::forward<Func>(func)).finally([this, it] {
            if (--it->second.users_num == 0) {
                _locks.erase(it);
            }
        });
    }
};

} // namespace seastar::fs
