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

#include "seastar/core/reactor.hh"
#include "seastar/core/posix.hh"

#include <cstdlib>
#include <string>
#include <unistd.h>
#include <utility>

namespace seastar::fs {

class temporary_directory {
    std::string _path;

public:
    explicit temporary_directory(std::string path) : _path(std::move(path) + ".XXXXXX") {
        char* ret = mkdtemp(_path.data());
        throw_system_error_on(ret == nullptr);
    }

    ~temporary_directory() {
        int ret = rmdir(_path.data());
        if (ret) {
            seastar_logger.warn("Failed to remove temporary directory {}", _path);
        }
    }

    temporary_directory(const temporary_directory&) = delete;
    temporary_directory& operator=(const temporary_directory&) = delete;
    temporary_directory(temporary_directory&&) noexcept = delete;
    temporary_directory& operator=(temporary_directory&&) noexcept = delete;

    const std::string& path() const noexcept {
        return _path;
    }
};

} // namespace seastar::fs
