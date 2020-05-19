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

#include "fs/backend/shard.hh"
#include "fs/bootstrap_record.hh"
#include "fs/inode.hh"
#include "fs/units.hh"
#include "fs_mock_block_device.hh"

#include "seastar/core/units.hh"
#include "seastar/fs/block_device.hh"
#include "seastar/testing/thread_test_case.hh"
#include "seastar/util/defer.hh"

using namespace seastar;
using namespace fs;

constexpr unit_size_t cluster_size = 1 * MB;
constexpr unit_size_t alignment = 4 * KB;
constexpr inode_t root_directory = 0;

future<std::set<std::string>> get_entries_from_directory(backend::shard& shard, std::string dir_path) {
    return async([&shard, dir_path = std::move(dir_path)] {
        std::set<std::string> entries;
        shard.iterate_directory(dir_path, [&entries] (const std::string& entry) -> future<stop_iteration> {
            entries.insert(entry);
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }).wait();
        return entries;
    });
}

BOOST_TEST_DONT_PRINT_LOG_VALUE(std::set<std::string>)

SEASTAR_THREAD_TEST_CASE(create_dirs_and_bootstrap_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    const bootstrap_record::shard_info shard_info({1, {1, 16}});
    const std::set<std::string> control_directories = {{"dir1", "dir2", "dir3"}};
    auto dev_impl = make_shared<mock_block_device_impl>();

    {
        block_device device(dev_impl);
        auto shard = backend::shard(std::move(device), cluster_size, alignment);
        const auto close_log = defer([&shard]() mutable { shard.shutdown().wait(); });
        shard.bootstrap(root_directory, shard_info.metadata_cluster, shard_info.available_clusters, 1, 0).wait();

        int flush_after = 1;
        for (auto directory: control_directories) {
            shard.create_directory("/" + std::move(directory), file_permissions::default_file_permissions).wait();
            if (--flush_after == 0) {
                shard.flush_log().wait();
            }
        }

        const auto entries = get_entries_from_directory(shard, "/").get0();
        BOOST_REQUIRE_EQUAL(entries, control_directories);
    }

    {
        block_device device(dev_impl);
        auto shard = backend::shard(std::move(device), cluster_size, alignment);
        const auto close_log = defer([&shard]() mutable { shard.shutdown().wait(); });
        shard.bootstrap(root_directory, shard_info.metadata_cluster, shard_info.available_clusters, 1, 0).wait();

        const auto entries = get_entries_from_directory(shard, "/").get0();
        BOOST_REQUIRE_EQUAL(entries, control_directories);
    }
}
