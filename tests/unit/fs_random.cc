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

#include "fs_random.hh"

namespace seastar::fs {

void random_overwrite(void* data, size_t len) {
    using NL = std::numeric_limits<uint8_t>;
    std::uniform_int_distribution<uint8_t> dist(NL::min(), NL::max());
    uint8_t* ptr = reinterpret_cast<uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
        ptr[i] = dist(testing::local_random_engine);
    }
}

} // namespace seastar::fs
