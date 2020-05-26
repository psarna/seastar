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

#include "filesystem_mgmt.hh"

#include <fmt/format.h>
#include <seastar/util/conversions.hh>
#include <stdexcept>
#include <string>
#include <utility>

std::pair<size_t, size_t> parse_memory_range(std::string s) {
    constexpr char delim = ',';
    auto pos = s.find(delim);
    if (pos == s.npos) {
        throw std::runtime_error(fmt::format("Cannot parse memory range '{}'", s));
    }
    return { seastar::parse_memory_size(s.substr(0, pos)), seastar::parse_memory_size(s.substr(pos + 1, s.size())) };
}

filesystem_type parse_fs_type(const std::string& fs) {
    if (fs == "xfs" || fs == "XFS") {
        return filesystem_type::XFS;
    } else if (fs == "ext4" || fs == "EXT4") {
        return filesystem_type::EXT4;
    }
    throw std::runtime_error(fmt::format("Unknown fs type '{}'", fs));
}
