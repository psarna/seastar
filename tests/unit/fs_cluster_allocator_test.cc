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

#define BOOST_TEST_MODULE fs

#include "fs/cluster.hh"
#include "fs/cluster_allocator.hh"
#include "seastar/core/circular_buffer.hh"

#include <random>
#include <seastar/core/units.hh>
#include <boost/test/included/unit_test.hpp>
#include <unordered_set>

using namespace seastar;
using namespace seastar::fs;

namespace {

// Check if a is subset of b
bool is_buff_subset(circular_buffer<cluster_id_t> a, circular_buffer<cluster_id_t> b) {
    if (a.size() > b.size()) {
        return false;
    }
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    return std::includes(b.begin(), b.end(), a.begin(), a.end());
}

circular_buffer<cluster_id_t> copy_buff(const circular_buffer<cluster_id_t>& circ) {
    circular_buffer<cluster_id_t> ret;
    for (auto& elem : circ) {
        ret.emplace_back(elem);
    }
    return ret;
}

} // namespace

BOOST_AUTO_TEST_CASE(test_cluster_0) {
    circular_buffer<cluster_id_t> tmp_buff;
    tmp_buff.emplace_back(0);
    cluster_allocator ca({}, std::move(tmp_buff));
    BOOST_REQUIRE_EQUAL(ca.alloc().value(), 0);
    BOOST_REQUIRE(ca.alloc() == std::nullopt);
    BOOST_REQUIRE(ca.alloc() == std::nullopt);
    ca.free(0);
    BOOST_REQUIRE_EQUAL(ca.alloc().value(), 0);
    BOOST_REQUIRE(ca.alloc() == std::nullopt);
    BOOST_REQUIRE(ca.alloc() == std::nullopt);
}

BOOST_AUTO_TEST_CASE(test_empty) {
    cluster_allocator empty_ca({}, {});
    BOOST_REQUIRE(empty_ca.alloc() == std::nullopt);
}

BOOST_AUTO_TEST_CASE(test_small) {
    constexpr cluster_id_t cluster_nb = 5;
    circular_buffer<cluster_id_t> buff;
    for (cluster_id_t i = 0; i < cluster_nb; i++) {
        buff.emplace_back(i);
    }

    cluster_allocator small_ca({}, copy_buff(buff));

    circular_buffer<cluster_id_t> tmp_buff;
    for (size_t i = 0; i < buff.size() - 1; ++i) {
        tmp_buff.emplace_back(small_ca.alloc().value());
    }

    BOOST_REQUIRE(is_buff_subset(copy_buff(tmp_buff), copy_buff(buff)));

    for (size_t i = 0; i < buff.size() - 1; ++i) {
        small_ca.free(tmp_buff[i]);
    }
    tmp_buff.clear();
    for (size_t i = 0; i < buff.size(); ++i) {
        tmp_buff.emplace_back(small_ca.alloc().value());
    }
    BOOST_REQUIRE(is_buff_subset(copy_buff(tmp_buff), copy_buff(buff)));
    BOOST_REQUIRE(small_ca.alloc() == std::nullopt);
}

BOOST_AUTO_TEST_CASE(test_max) {
    constexpr cluster_id_t cluster_nb = 1024;

    circular_buffer<cluster_id_t> buff;
    for (cluster_id_t i = 0; i < cluster_nb; i++) {
        buff.emplace_back(i);
    }
    cluster_allocator ordinary_ca({}, copy_buff(buff));

    circular_buffer<cluster_id_t> tmp_buff;
    for (cluster_id_t i = 0; i < cluster_nb; i++) {
        auto cluster_id = ordinary_ca.alloc();
        BOOST_REQUIRE(cluster_id != std::nullopt);
        tmp_buff.emplace_back(cluster_id.value());
    }
    BOOST_REQUIRE(is_buff_subset(copy_buff(tmp_buff), copy_buff(buff)));

    BOOST_REQUIRE(ordinary_ca.alloc() == std::nullopt);
    for (cluster_id_t i = 0; i < cluster_nb; i++) {
        ordinary_ca.free(i);
    }
}

BOOST_AUTO_TEST_CASE(test_random_action) {
    constexpr cluster_id_t cluster_nb = 8;
    constexpr size_t repeats = 1000;

    std::default_random_engine random_engine;
    auto generate_random_value = [&](size_t min, size_t max) {
        return std::uniform_int_distribution<size_t>(min, max)(random_engine);
    };

    circular_buffer<cluster_id_t> alloc_vec;
    std::unordered_set<cluster_id_t> remaining;
    for (cluster_id_t i = 0; i < cluster_nb; ++i) {
        alloc_vec.emplace_back(i);
        remaining.emplace(i);
    }
    cluster_allocator ca({}, std::move(alloc_vec));
    alloc_vec.clear();

    for (size_t i = 0; i < repeats; ++i) {
        if (remaining.size() > 0 && (generate_random_value(0, 1) || alloc_vec.size() == 0)) {
            auto cluster_id = ca.alloc();
            BOOST_REQUIRE(cluster_id != std::nullopt);

            auto remaining_it = remaining.find(*cluster_id);
            BOOST_REQUIRE(remaining_it != remaining.end());
            alloc_vec.emplace_back(cluster_id.value());
            remaining.erase(remaining_it);
        } else {
            size_t elem_id = generate_random_value(0, alloc_vec.size() - 1);
            ca.free(alloc_vec[elem_id]);
            remaining.emplace(alloc_vec[elem_id]);

            std::swap(alloc_vec[elem_id], alloc_vec.back());
            alloc_vec.pop_back();
        }
    }
}
