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

#include "fs/backend/cluster_allocator.hh"

#include <cassert>

namespace seastar::fs::backend {

cluster_allocator::cluster_allocator() : _cluster_sem(0) {}

cluster_allocator::cluster_allocator(std::unordered_set<cluster_id_t> allocated_clusters,
        circular_buffer<cluster_id_t> free_clusters)
        : cluster_allocator() {
    reset(std::move(allocated_clusters), std::move(free_clusters));
}

void cluster_allocator::reset(std::unordered_set<cluster_id_t> allocated_clusters,
        circular_buffer<cluster_id_t> free_clusters) {
    assert((size_t)_cluster_sem.available_units() == _free_clusters.size());
    assert(_cluster_sem.waiters() == 0);

    free_clusters.reserve(free_clusters.size() + allocated_clusters.size());
    _allocated_clusters = [&] {
        std::unordered_map<cluster_id_t, bool> allocated_clusters_tmp;
        for (auto& i : allocated_clusters) {
            allocated_clusters_tmp.emplace(i, true);
        }
        for (auto& i : free_clusters) {
            allocated_clusters_tmp.emplace(i, false);
        }
        return allocated_clusters_tmp;
    }();
    _free_clusters = std::move(free_clusters);

    _cluster_sem.consume(_cluster_sem.available_units());
    _cluster_sem.signal(_free_clusters.size());
}

cluster_id_t cluster_allocator::do_alloc() noexcept {
    assert(!_free_clusters.empty());

    cluster_id_t cluster_id = _free_clusters.front();
    _free_clusters.pop_front();
    _allocated_clusters[cluster_id] = true;

    return cluster_id;
}

void cluster_allocator::do_free(cluster_id_t cluster_id) noexcept {
    assert(_allocated_clusters.count(cluster_id) == 1);
    _free_clusters.emplace_back(cluster_id);
    _allocated_clusters[cluster_id] = false;
}

std::optional<cluster_id_t> cluster_allocator::alloc() noexcept {
    if (_cluster_sem.available_units() == 0) {
        return std::nullopt;
    }
    _cluster_sem.consume(1);

    assert(_free_clusters.size() > 0);

    return do_alloc();
}

future<std::vector<cluster_id_t>> cluster_allocator::alloc_wait(size_t count) {
    std::vector<cluster_id_t> cluster_ids;
    cluster_ids.reserve(count);
    return _cluster_sem.wait(count).then([this, count, cluster_ids = std::move(cluster_ids)]() mutable {
        for (size_t i = 0; i < count; ++i) {
            cluster_ids.emplace_back(do_alloc());
        }
        return make_ready_future<std::vector<cluster_id_t>>(std::move(cluster_ids));
    });
}

void cluster_allocator::free(cluster_id_t cluster_id) noexcept {
    do_free(cluster_id);
    _cluster_sem.signal();
}

void cluster_allocator::free(const std::vector<cluster_id_t>& cluster_ids) noexcept {
    for (auto& cluster_id : cluster_ids) {
        do_free(cluster_id);
    }
    _cluster_sem.signal(cluster_ids.size());
}

} // namespace seastar::fs::backend
