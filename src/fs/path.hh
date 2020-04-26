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

#include <string>

namespace seastar::fs::path {

// Extracts the last component in @p path. WARNING: The last component is empty iff @p path is empty or ends with '/'
inline std::string extract_last_component(std::string& path) {
    auto beg = path.find_last_of('/');
    if (beg == path.npos) {
        std::string res = std::move(path);
        path = {};
        return res;
    }

    auto res = path.substr(beg + 1);
    path.resize(beg + 1);
    return res;
}

// Returns first non-root component.
std::string root_entry(const std::string& path);

std::string canonical(std::string path, std::string curr_dir = "/");

bool is_canonical(const std::string& path) noexcept;

} // namespace seastar::fs
