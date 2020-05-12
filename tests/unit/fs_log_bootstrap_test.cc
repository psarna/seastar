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

#include "fs/bootstrap_record.hh"
#include "fs/inode.hh"
#include "fs/metadata_log.hh"
#include "fs/units.hh"
#include "fs_mock_block_device.hh"

#include "seastar/core/units.hh"
#include "seastar/fs/block_device.hh"
#include "seastar/fs/temporary_file.hh"
#include "seastar/testing/thread_test_case.hh"
#include "seastar/util/defer.hh"

using namespace seastar;
using namespace fs;

constexpr unit_size_t cluster_size = 1 * MB;
constexpr unit_size_t alignment = 4 * KB;
constexpr inode_t root_directory = 0;
constexpr auto device_path = "/tmp/seastarfs";
constexpr disk_offset_t device_size = 64 * MB;

future<std::set<std::string>> get_entries_from_directory(metadata_log& log, std::string dir_path) {
    return async([&log, dir_path = std::move(dir_path)] {
        std::set<std::string> entries;
        log.iterate_directory(dir_path, [&entries] (const std::string& entry) -> future<stop_iteration> {
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
        auto log = metadata_log(std::move(device), cluster_size, alignment);
        const auto close_log = defer([&log]() mutable { log.shutdown().wait(); });
        log.bootstrap(root_directory, shard_info.metadata_cluster, shard_info.available_clusters, 1, 0).wait();

        int flush_after = 1;
        for (auto directory: control_directories) {
            log.create_directory("/" + std::move(directory), file_permissions::default_file_permissions).wait();
            if (--flush_after == 0) {
                log.flush_log().wait();
            }
        }

        const auto entries = get_entries_from_directory(log, "/").get0();
        BOOST_REQUIRE_EQUAL(entries, control_directories);
    }

    {
        block_device device(dev_impl);
        auto log = metadata_log(std::move(device), cluster_size, alignment);
        const auto close_log = defer([&log]() mutable { log.shutdown().wait(); });
        log.bootstrap(root_directory, shard_info.metadata_cluster, shard_info.available_clusters, 1, 0).wait();

        const auto entries = get_entries_from_directory(log, "/").get0();
        BOOST_REQUIRE_EQUAL(entries, control_directories);
    }
}

SEASTAR_THREAD_TEST_CASE(create_file_write_and_bootstrap_test) {
    BOOST_TEST_MESSAGE("\nTest name: " << get_name());
    const std::string file_name = "file";
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size * 2);
    const bootstrap_record::shard_info shard_info({1, {1, device_size * 2 / alignment}});

    {
        block_device device = open_block_device(tf.path()).get0();
        auto log = metadata_log(std::move(device), cluster_size, alignment);
        auto close_log = defer([&log]() mutable { log.shutdown().get(); });
        log.bootstrap(root_directory, shard_info.metadata_cluster, shard_info.available_clusters, 1, 0).get();

        inode_t inode = log.create_and_open_file("/" + file_name, file_permissions::default_file_permissions).get0();
        auto close_inode = defer([&log, inode] { log.close_file(inode).get(); });

        parallel_for_each(boost::irange<unit_size_t>(0, device_size / alignment), [&log, inode](unit_size_t i) {
            auto wbuf = allocate_aligned_buffer<uint8_t>(alignment, alignment);
            std::fill(wbuf.get(), wbuf.get() + alignment, i);

            return log.write(inode, i * alignment, wbuf.get(), alignment).then([wbuf = std::move(wbuf)](size_t ret) {
                BOOST_REQUIRE_EQUAL(ret, alignment);
            });
        }).get();

        log.flush_log().get();
    }

    {
        block_device device = open_block_device(tf.path()).get0();
        auto log = metadata_log(std::move(device), cluster_size, alignment);
        auto close_log = defer([&log]() mutable { log.shutdown().get(); });
        log.bootstrap(root_directory, shard_info.metadata_cluster, shard_info.available_clusters, 1, 0).get();

        const auto entries = get_entries_from_directory(log, "/").get0();
        BOOST_REQUIRE_EQUAL(entries.size(), 1);

        inode_t inode = log.open_file("/" + file_name).get0();
        auto close_inode = defer([&log, inode] { log.close_file(inode).get(); });

        file_offset_t file_size = log.file_size(inode);
        BOOST_REQUIRE_EQUAL(file_size, device_size);
    }
}
