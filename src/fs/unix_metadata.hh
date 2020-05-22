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

#include "seastar/core/file-types.hh"

#include <cstdint>
#include <sys/types.h>

namespace seastar::fs {

enum class file_type : uint8_t {
    REGULAR_FILE,
    DIRECTORY,
};

struct unix_metadata {
    file_type ftype;
    file_permissions perms;
    uid_t uid;
    gid_t gid;
    uint64_t btime_ns;
    uint64_t mtime_ns;
    uint64_t ctime_ns;
};

} // namespace seastar::fs
