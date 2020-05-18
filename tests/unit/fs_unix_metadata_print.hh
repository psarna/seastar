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

#include "fs/unix_metadata.hh"

#include <ostream>

namespace seastar::fs {

inline std::ostream& operator<<(std::ostream& os, const unix_metadata& x) {
    os << '{';
    os << "ftype=";
    switch (file_type(x.ftype)) {
    case file_type::DIRECTORY: os << "DIR,"; break;
    case file_type::REGULAR_FILE: os << "REG,"; break;
    }

    os << "perms=";
    using fp = file_permissions;
    auto isset = [&x](file_permissions p) { return (x.perms & p) == p; };
    os << (isset(fp::user_read) ? 'r' : '-');
    os << (isset(fp::user_write) ? 'w' : '-');
    os << (isset(fp::user_execute) ? 'x' : '-');
    os << (isset(fp::group_read) ? 'r' : '-');
    os << (isset(fp::group_write) ? 'w' : '-');
    os << (isset(fp::group_execute) ? 'x' : '-');
    os << (isset(fp::others_read) ? 'r' : '-');
    os << (isset(fp::others_write) ? 'w' : '-');
    os << (isset(fp::others_execute) ? 'x' : '-');
    os << ',';

    os << "uid=" << x.uid << ',';
    os << "gid=" << x.gid << ',';
    os << "btime=" << x.btime_ns << ',';
    os << "mtime=" << x.mtime_ns << ',';
    os << "ctime=" << x.ctime_ns << '}';
    return os;
}

} // namespace seastar::fs
