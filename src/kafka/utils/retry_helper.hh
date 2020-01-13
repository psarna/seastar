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
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#pragma once

#include <cstdint>
#include <cmath>
#include <random>
#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/bool_class.hh>

namespace seastar {

namespace kafka {

struct do_retry_tag { };
using do_retry = bool_class<do_retry_tag>;

class retry_helper {
private:
    uint32_t _max_retry_count;
    float _base_ms;
    uint32_t _max_backoff_ms;

    std::random_device _rd;
    std::mt19937 _mt;

    template<typename AsyncAction>
    future<> with_retry(AsyncAction&& action, uint32_t retry_number) {
        if (retry_number >= _max_retry_count) {
            return make_ready_future<>();
        }
        return backoff(retry_number)
        .then([this, action = std::forward<AsyncAction>(action), retry_number]() mutable {
            return futurize_apply(action)
            .then([this, action = std::forward<AsyncAction>(action), retry_number](auto do_retry_val) mutable {
                if (do_retry_val == do_retry::yes) {
                    return with_retry(std::forward<AsyncAction>(action), retry_number + 1);
                } else {
                    return make_ready_future<>();
                }
            });
        });
    }

    future<> backoff(uint32_t retry_number) {
        if (retry_number == 0) {
            return make_ready_future<>();
        }

        // Exponential backoff with (full) jitter
        auto backoff_time = _base_ms * std::pow(2.0f, retry_number - 1);
        auto backoff_time_discrete = static_cast<uint32_t>(std::round(backoff_time));

        auto capped_backoff_time = std::min(_max_backoff_ms, backoff_time_discrete);
        std::uniform_int_distribution<uint32_t> dist(0, capped_backoff_time);

        auto jittered_backoff = dist(_mt);
        return seastar::sleep(std::chrono::milliseconds(jittered_backoff));
    }

public:
    retry_helper(uint32_t max_retry_count, float base_ms, uint32_t max_backoff_ms)
        : _max_retry_count(max_retry_count), _base_ms(base_ms), _max_backoff_ms(max_backoff_ms), _mt(_rd()) {}

    template<typename AsyncAction>
    future<> with_retry(AsyncAction&& action) {
        return with_retry(std::forward<AsyncAction>(action), 0);
    }
};

}

}
