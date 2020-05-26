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

#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/file-types.hh"
#include "seastar/core/file.hh"
#include "seastar/core/thread.hh"
#include "seastar/core/units.hh"
#include "seastar/fs/file.hh"
#include "seastar/fs/filesystem.hh"
#include "seastar/fs/temporary_file.hh"
#include "seastar/fs/units.hh"
#include "seastar/testing/thread_test_case.hh"
#include "seastar/util/defer.hh"

#include "fs_mock_block_device.hh"

using namespace seastar;
using namespace fs;

constexpr auto device_path = "/tmp/seastarfs";
constexpr auto device_size = 32 * MB;

constexpr uint64_t version = 1;
constexpr unit_size_t cluster_size = 1 * MB;
constexpr unit_size_t alignment = 4 * KB;
constexpr inode_t root_directory = 0;

future<> run_parallel_read_write(file& f, size_t _device_size, const unit_size_t _alignment) {
    return parallel_for_each(boost::irange<off_t>(0, _device_size / _alignment),
            [&f, _alignment](off_t i) {
        auto wbuf = allocate_aligned_buffer<unsigned char>(_alignment, _alignment);
        std::fill(wbuf.get(), wbuf.get() + _alignment, i);
        auto wb = wbuf.get();

        return f.dma_write(i * _alignment, wb, _alignment).then(
                [&f, i, wbuf = std::move(wbuf), _alignment](size_t ret) mutable {
            BOOST_REQUIRE_EQUAL(ret, _alignment);
            auto rbuf = allocate_aligned_buffer<unsigned char>(_alignment, _alignment);
            auto rb = rbuf.get();
            return f.dma_read(i * _alignment, rb, alignment).then(
                [f, rbuf = std::move(rbuf), wbuf = std::move(wbuf), _alignment](auto ret) {
                    BOOST_REQUIRE_EQUAL(ret, _alignment);
                    BOOST_REQUIRE(std::equal(rbuf.get(), rbuf.get() + _alignment, wbuf.get()));
                });
        });
    });
}

SEASTAR_THREAD_TEST_CASE(local_shard_parallel_read_write_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size * 2);
    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, 1).get();
    const std::string filename = "abc.txt";

    {
        sharded<filesystem> fs;
        fs::bootfs(fs, tf.path(), -1, 0).get();
        auto stop_fs = defer([&fs] { fs.stop().get(); });

        file f = fs.local().create_and_open_file("/" + filename, open_flags::rw).get0();
        auto close_file = defer([&f] { f.close().get(); });

        run_parallel_read_write(f, device_size, alignment).get();

        f.flush().get();
    }
    {
        sharded<filesystem> fs;
        fs::bootfs(fs, tf.path(), -1, 0).get();
        auto stop_fs = defer([&fs] { fs.stop().get(); });

        auto entries = fs.local().local_root().get0();
        BOOST_REQUIRE(entries.find(filename) != entries.end());
        /* TODO check file size */
    }
}

SEASTAR_THREAD_TEST_CASE(foreign_shard_basic_read_write_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size * smp::count * 2);
    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, smp::count).get();
    static const std::string directory = "test";
    static const std::string filename = "/" + directory + "/abc.txt";

    {
        sharded<filesystem> fs;
        fs::bootfs(fs, tf.path(), -1, 0).get();
        auto stop_fs = defer([&fs] { fs.stop().get(); });

        fs.local().create_directory("/" + directory).get();
        smp::submit_to(smp::count - 1, [&fs] {
            return async([&fs] {
                file f = fs.local().create_and_open_file(filename, open_flags::rw).get0();
                auto close_file = defer([&f] { f.close().get(); });

                run_parallel_read_write(f, device_size, alignment).get();

                f.flush().get();
            });
        }).get();
    }
    {
        sharded<filesystem> fs;
        fs::bootfs(fs, tf.path(), -1, 0).get();
        auto stop_fs = defer([&fs] { fs.stop().get(); });

        auto entries = fs.local().local_root().get0();
        BOOST_REQUIRE(entries.find(directory) != entries.end());
        /* TODO check file size */
    }
}

SEASTAR_THREAD_TEST_CASE(valid_basic_create_directory_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size);

    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, smp::count).get();

    {
        sharded<filesystem> fs;
        fs::bootfs(fs, tf.path(), -1, 0).get();
        auto stop_fs = defer([&fs] { fs.stop().get(); });

        fs.invoke_on(smp::count - 1, &filesystem::create_directory, "/test").get();
        fs.local().create_directory("/test").get();

        const shared_entries foreign_entries = fs.invoke_on(smp::count - 1, &filesystem::local_root).get0();
        BOOST_REQUIRE_EQUAL(foreign_entries.size(), 1);

        const shared_entries local_entries = fs.local().local_root().get0();
        BOOST_REQUIRE_EQUAL(local_entries.size(), 0);
    }

    {
        sharded<filesystem> fs;
        fs::bootfs(fs, tf.path(), -1, 0).get();
        auto stop_fs = defer([&fs] { fs.stop().get(); });

        const shared_entries entries = fs.local().global_root().get0();
        BOOST_REQUIRE_EQUAL(entries.size(), 1);
    }
}

SEASTAR_THREAD_TEST_CASE(valid_parallel_create_directory_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size);

    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, smp::count).get();

    {
        sharded<filesystem> fs;
        fs::bootfs(fs, tf.path(), -1, 0).get();
        auto stop_fs = defer([&fs] { fs.stop().get(); });

        fs.invoke_on_all([](filesystem& fs) mutable {
            return fs.create_directory("/test");
        }).get();

        const shared_entries global_entries = fs.local().global_root().get0();
        BOOST_REQUIRE_EQUAL(global_entries.size(), 1);
    }
}

SEASTAR_THREAD_TEST_CASE(valid_basic_create_file_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size);

    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, smp::count).get();

    {
        sharded<filesystem> fs;
        fs::bootfs(fs, tf.path(), -1, 0).get();
        auto stop_fs = defer([&fs] { fs.stop().get(); });

        file f = fs.local().create_and_open_file("/test", {}).get0();
        auto close_file = defer([&f] { f.close().get(); });

        const shared_entries entries = fs.local().local_root().get0();
        BOOST_REQUIRE_EQUAL(entries.size(), 1);

        f.flush().get0();
    }

    {
        sharded<filesystem> fs;
        fs::bootfs(fs, tf.path(), -1, 0).get();
        auto stop_fs = defer([&fs] { fs.stop().get(); });

        const shared_entries entries = fs.local().global_root().get0();
        BOOST_REQUIRE_EQUAL(entries.size(), 1);
    }
}

BOOST_TEST_DONT_PRINT_LOG_VALUE(bootstrap_record)

SEASTAR_THREAD_TEST_CASE(valid_path_mkfs_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(device_size);

    const std::vector<bootstrap_record::shard_info> shards_info({{1,  {1,  device_size / MB}}});

    const bootstrap_record write_record(
        version, alignment, cluster_size, root_directory, shards_info
    );

    auto dev = open_block_device(tf.path()).get0();

    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, write_record.shards_nb()).get();

    const auto read_record = bootstrap_record::read_from_disk(dev).get0();
    dev.close().get();

    BOOST_REQUIRE_EQUAL(write_record, read_record);
}

SEASTAR_THREAD_TEST_CASE(valid_cluster_distribution_mkfs_test) {
    const auto tf = temporary_file(device_path);
    tf.truncate(16 * MB);

    const std::vector<bootstrap_record::shard_info> shards_info({
        { 1,  { 1,  4  } }, // 3
        { 4,  { 4,  7  } }, // 3
        { 7,  { 7,  10 } }, // 3
        { 10, { 10, 12 } }, // 2
        { 12, { 12, 14 } }, // 2
        { 14, { 14, 16 } }, // 2
    });

    const bootstrap_record write_record(version, alignment, cluster_size, root_directory, shards_info);

    auto dev = open_block_device(tf.path()).get0();
    auto close_dev = defer([&dev] { dev.close().get(); });

    fs::mkfs(tf.path(), version, cluster_size, alignment, root_directory, write_record.shards_nb()).get();

    const auto read_record = bootstrap_record::read_from_disk(dev).get0();

    BOOST_REQUIRE_EQUAL(write_record, read_record);
}
