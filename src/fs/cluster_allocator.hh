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

#include "fs/cluster.hh"
#include "seastar/core/circular_buffer.hh"
#include "seastar/core/future.hh"
#include "seastar/core/semaphore.hh"

#include <cstdint>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace seastar::fs {

class cluster_allocator {
    std::unordered_map<cluster_id_t, bool> _allocated_clusters;
    circular_buffer<cluster_id_t> _free_clusters;
    semaphore _cluster_sem;

    cluster_id_t do_alloc() noexcept;
    void do_free(cluster_id_t cluster_id) noexcept;

public:
    cluster_allocator();
    cluster_allocator(std::unordered_set<cluster_id_t> allocated_clusters, circular_buffer<cluster_id_t> free_clusters);

    cluster_allocator(const cluster_allocator&) = delete;
    cluster_allocator& operator=(const cluster_allocator&) = delete;
    cluster_allocator(cluster_allocator&&) = default;
    cluster_allocator& operator=(cluster_allocator&&) = delete;

    // Changes cluster_allocator's set of free and allocated clusters. Assumes that there are no allocated clusters
    // and nobody uses the cluster_allocator (e.g. waiting to alloc() a cluster).
    // Strong exception guarantee is provided.
    void reset(std::unordered_set<cluster_id_t> allocated_clusters, circular_buffer<cluster_id_t> free_clusters);

    // Tries to allocate a cluster.
    std::optional<cluster_id_t> alloc() noexcept;

    // Waits until @p count free clusters are available and allocates them.
    // Strong exception guarantee is provided.
    future<std::vector<cluster_id_t>> alloc_wait(size_t count = 1);

    // @p cluster_id has to be allocated using alloc() or alloc_wait()
    void free(cluster_id_t cluster_id) noexcept;

    // @p cluster_id has to be allocated using alloc() or alloc_wait()
    void free(const std::vector<cluster_id_t>& cluster_ids) noexcept;

    // Returns the total number of clusters
    cluster_id_t size() const;

    // Returns the number of allocated clusters
    cluster_id_t allocated_size() const;
};

} // namespace seastar::fs
