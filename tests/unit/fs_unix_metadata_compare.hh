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

namespace seastar::fs {

constexpr bool operator==(const unix_metadata& a, const unix_metadata& b) noexcept {
    return a.perms == b.perms && a.ftype == b.ftype  && a.uid == b.uid && a.gid == b.gid && a.btime_ns == b.btime_ns && a.mtime_ns == b.mtime_ns && a.ctime_ns == b.ctime_ns;
}
constexpr bool operator!=(const unix_metadata& a, const unix_metadata& b) noexcept {
    return !(a == b);
}

} // namespace seastar::fs
