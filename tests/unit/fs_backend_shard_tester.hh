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

#include "fs/backend/metadata_log/to_disk_buffer.hh"
#include "fs/backend/shard.hh"
#include "fs_backend_metadata_log_to_disk_buffer_mocker.hh"
#include "fs_block_device_mocker.hh"
#include "fs_freezing_clock.hh"
#include "fs_random.hh"
#include "seastar/core/print.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/units.hh"
#include "seastar/fs/block_device.hh"
#include "seastar/fs/unit_types.hh"
#include "seastar/testing/test_runner.hh"

#include <boost/test/unit_test.hpp>

namespace seastar::fs::backend {

class shard_tester {
public:
    struct options {
        disk_offset_t cluster_size = 1 * MB;
        disk_offset_t alignment = 256;
        cluster_id_t first_metadata_cluster_id = 1;
        cluster_range available_clusters = {
            .beg = 1,
            .end = 10,
        };
        fs_shard_id_t fs_shards_pool_size = 1;
        fs_shard_id_t fs_shard_id = 0;
        inode_t root_dir = 0;
    };

    static constexpr options default_options() noexcept { return {}; }

    const options options;
    shared_ptr<block_device_mocker_impl> device_holder;
    block_device_mocker_impl& device;
    shared_ptr<std::vector<shared_ptr<metadata_log::to_disk_buffer_mocker>>> ml_buffers_holder;
    std::vector<shared_ptr<metadata_log::to_disk_buffer_mocker>>& ml_buffers;
    shared_ptr<FreezingClock> clock_holder;
    FreezingClock& clock;
    class shard shard;

    explicit shard_tester(const struct options& options = default_options());

    void bootstrap_shard() { bootstrap_shard(shard); }

    void bootstrap_shard(backend::shard& dest_shard);

    metadata_log::to_disk_buffer_mocker& curr_ml_buff() const noexcept {
        return *ml_buffers.back().get();
    }
};

} // namespace seastar::fs::backend
