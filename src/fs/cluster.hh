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

#include "seastar/fs/bitwise.hh"
#include "seastar/fs/units.hh"

namespace seastar::fs {

using cluster_id_t = uint64_t;
using cluster_range = range<cluster_id_t>;

inline cluster_id_t offset_to_cluster_id(disk_offset_t offset, unit_size_t cluster_size) noexcept {
    assert(is_power_of_2(cluster_size));
    return div_by_power_of_2(offset, cluster_size);
}

inline disk_offset_t cluster_id_to_offset(cluster_id_t cluster_id, unit_size_t cluster_size) noexcept {
    assert(is_power_of_2(cluster_size));
    return mul_by_power_of_2(cluster_id, cluster_size);
}

} // namespace seastar::fs
