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

#define BOOST_TEST_MODULE fs

#include "fs/cluster_allocator.hh"

#include <boost/test/included/unit_test.hpp>
#include <deque>
#include <queue>
#include <seastar/core/units.hh>
#include <unordered_set>

using namespace seastar;


BOOST_AUTO_TEST_CASE(cluster_allocator) {
    constexpr fs::cluster_id_t clusters_per_shard = 1024;
    std::unordered_set<fs::cluster_id_t> empty_uset;
    std::queue<fs::cluster_id_t> empty_queue;
    fs::cluster_allocator empty_ca{empty_uset, empty_queue};
    BOOST_REQUIRE_EQUAL(empty_ca.alloc(), 0);
    std::deque<fs::cluster_id_t> deq{1, 5, 3, 4, 2};
    fs::cluster_allocator small_ca(empty_uset, std::queue<fs::cluster_id_t>{deq});
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[0]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[1]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[2]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[3]);
    small_ca.free(deq[2]);
    small_ca.free(deq[1]);
    small_ca.free(deq[3]);
    small_ca.free(deq[0]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[4]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[2]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[1]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[3]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[0]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), 0);
    small_ca.free(deq[2]);
    small_ca.free(deq[4]);
    small_ca.free(deq[3]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[2]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[4]);
    small_ca.free(deq[2]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[3]);
    small_ca.free(deq[4]);
    BOOST_REQUIRE_EQUAL(small_ca.alloc(), deq[2]);

    std::queue<fs::cluster_id_t> q;
    for (fs::cluster_id_t i = 0; i < clusters_per_shard; i++) {
        q.push(i);
    }
    fs::cluster_allocator ordinary_ca(empty_uset, q);
    for (fs::cluster_id_t i = 0; i < clusters_per_shard; i++) {
        BOOST_REQUIRE_EQUAL(ordinary_ca.alloc(), i);
    }
    for (fs::cluster_id_t i = 0; i < clusters_per_shard; i++) {
        ordinary_ca.free(i);
    }
    std::unordered_set<fs::cluster_id_t> uset;
    std::queue<fs::cluster_id_t>().swap(q);
    fs::cluster_id_t elem = 215;
    while (elem != 806) {
        q.push(elem);
        elem = (elem * 215) % 1021;
    }
    elem = 1;
    while (elem != 1020) {
        uset.insert(elem);
        elem = (elem * 19) % 1021;
    }
    fs::cluster_allocator random_ca(uset, q);
    elem = 215;
    while (elem != 1) {
        BOOST_REQUIRE_EQUAL(random_ca.alloc(), elem);
        random_ca.free(1021-elem);
        elem = (elem * 215) % 1021;
    }
}
