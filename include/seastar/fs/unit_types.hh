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

#include "seastar/fs/range.hh"

#include <cstdint>

namespace seastar::fs {

using disk_offset_t = uint64_t;
using disk_range = range<disk_offset_t>;

using file_offset_t = uint64_t;
using file_range = range<file_offset_t>;

using fs_shard_id_t = uint32_t;

using inode_t = uint64_t;

using cluster_id_t = uint64_t;
using cluster_range = range<cluster_id_t>;

} // namespace seastar::fs
