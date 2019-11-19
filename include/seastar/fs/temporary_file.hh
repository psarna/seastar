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

#include "seastar/core/posix.hh"

#include <string>

namespace seastar::fs {

class temporary_file {
    std::string _path;

public:
    explicit temporary_file(std::string path) : _path(std::move(path) + ".XXXXXX") {
        int fd = mkstemp(_path.data());
        throw_system_error_on(fd == -1);
        close(fd);
    }

    ~temporary_file() {
        unlink(_path.data());
    }

    temporary_file(const temporary_file&) = delete;
    temporary_file& operator=(const temporary_file&) = delete;
    temporary_file(temporary_file&&) noexcept = delete;
    temporary_file& operator=(temporary_file&&) noexcept = delete;

    const std::string& path() const noexcept {
        return _path;
    }
};

} // namespace seastar::fs
