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

#include "seastar/fs/unit_types.hh"

#include <deque>
#include <optional>
#include <unordered_set>

namespace seastar::fs::backend {

class cluster_allocator {
    // TODO: maybe optimize it using bitset?
    std::unordered_set<cluster_id_t> _allocated_clusters;
    std::deque<cluster_id_t> _free_clusters;

public:
    explicit cluster_allocator(std::unordered_set<cluster_id_t> allocated_clusters, std::deque<cluster_id_t> free_clusters);

    // Tries to allocate a cluster
    std::optional<cluster_id_t> alloc();

    // @p cluster_id has to be allocated using alloc()
    void free(cluster_id_t cluster_id);

    size_t remaining_clusters_number() const noexcept {
        return _free_clusters.size();
    }
};

} // namespace seastar::fs::backend
