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

#include "fs_backend_shard_tester.hh"
#include "fs_block_device_mocker.hh"
#include "seastar/core/file-types.hh"
#include "seastar/core/future.hh"
#include "seastar/fs/block_device.hh"
#include "seastar/testing/thread_test_case.hh"
#include "seastar/util/defer.hh"

using std::vector;
using std::string;

using namespace seastar;
using namespace seastar::fs;
namespace mle = seastar::fs::metadata_log::entries;

vector<string> get_entries_from_dir(backend::shard& shard, string dir_path) {
    vector<string> entries;
    shard.iterate_directory(dir_path, [&entries](const string& entry) {
        entries.emplace_back(entry);
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }).get();
    return entries;
}

SEASTAR_THREAD_TEST_CASE(create_dirs_and_bootstrap_test) {
    struct tester : backend::shard_tester {
        void test() {
            const vector<string> dirs = {"dir1", "dir2", "dir3"};
            constexpr auto perms = file_permissions::default_dir_permissions;
            shard.create_directory("/" + dirs[0], perms).get();
            shard.flush_log().get();
            shard.create_directory("/" + dirs[1], perms).get();
            shard.create_directory("/" + dirs[2], perms).get();
            shard.shutdown().get();
            // Bootstrapping the same shard again
            bootstrap_shard();
            BOOST_REQUIRE_EQUAL(get_entries_from_dir(shard, "/"), dirs);
            // Bootstrapping new shard
            backend::shard other_shard(block_device(device_holder), options.cluster_size, options.alignment);
            bootstrap_shard(other_shard);
            BOOST_REQUIRE_EQUAL(get_entries_from_dir(other_shard, "/"), dirs);
        }
    };

    tester().test();
}
