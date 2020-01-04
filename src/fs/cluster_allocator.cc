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

#include "fs/cluster.hh"
#include "fs/cluster_allocator.hh"

#include <cassert>
#include <optional>

namespace seastar {

namespace fs {

cluster_allocator::cluster_allocator(std::unordered_set<cluster_id_t> allocated_clusters, std::deque<cluster_id_t> free_clusters)
        : _allocated_clusters(std::move(allocated_clusters)), _free_clusters(std::move(free_clusters)) {}

std::optional<cluster_id_t> cluster_allocator::alloc() {
    if (_free_clusters.empty()) {
        return std::nullopt;
    }

    cluster_id_t cluster_id = _free_clusters.front();
    _free_clusters.pop_front();
    _allocated_clusters.insert(cluster_id);
    return cluster_id;
}

void cluster_allocator::free(cluster_id_t cluster_id) {
    assert(_allocated_clusters.count(cluster_id) == 1);
    _free_clusters.emplace_back(cluster_id);
    _allocated_clusters.erase(cluster_id);
}

}

}
