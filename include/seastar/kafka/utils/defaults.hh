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

#include <functional>
#include <memory>

#include "../../../../src/kafka/utils/partitioner.hh"

#include <seastar/core/future.hh>

namespace seastar {

namespace kafka {

namespace defaults {

std::function<future<>(uint32_t)> exp_retry_backoff(uint32_t base_ms, uint32_t max_backoff_ms);

std::unique_ptr<partitioner> round_robin_partitioner();
std::unique_ptr<partitioner> random_partitioner();

}

}

}