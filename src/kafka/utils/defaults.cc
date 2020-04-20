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

#include <cmath>
#include <random>
#include <chrono>

#include <seastar/kafka/utils/defaults.hh>
#include <seastar/core/sleep.hh>

namespace seastar {

namespace kafka {

namespace defaults {

std::function<future<>(uint32_t)> exp_retry_backoff(uint32_t base_ms, uint32_t max_backoff_ms) {
    std::random_device rd;
    return [base_ms, max_backoff_ms, mt = std::mt19937(rd())] (uint32_t retry_number) mutable {
        if (retry_number == 0) {
            return make_ready_future<>();
        }

        // Exponential backoff with (full) jitter
        auto backoff_time = base_ms * std::pow(2.0f, retry_number - 1);
        auto backoff_time_discrete = static_cast<uint32_t>(std::round(backoff_time));

        auto capped_backoff_time = std::min(max_backoff_ms, backoff_time_discrete);
        std::uniform_int_distribution<uint32_t> dist(0, capped_backoff_time);

        auto jittered_backoff = dist(mt);
        return seastar::sleep(std::chrono::milliseconds(jittered_backoff));
    };
}

std::unique_ptr<partitioner> round_robin_partitioner() {
    return std::make_unique<rr_partitioner>();
}

std::unique_ptr<partitioner> random_partitioner() {
    return std::make_unique<basic_partitioner>();
}

}

}

}