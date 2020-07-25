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
 * Copyright (C) 2020 ScyllaDB Ltd.
 */

#include "fs_backend_metadata_log_to_disk_buffer_mocker.hh"
#include "fs_backend_shard_tester.hh"
#include "fs_block_device_mocker.hh"
#include "fs_freezing_clock.hh"

namespace seastar::fs::backend {

namespace {
logger mlogger("fs_backend_shard_tester");
} // namespace

shard_tester::shard_tester(const struct shard_tester::options& options)
: options(options)
, device_holder(make_shared<block_device_mocker_impl>(options.alignment))
, device(*device_holder.get())
, ml_buffers_holder(make_shared<decltype(ml_buffers_holder)::element_type>())
, ml_buffers(*ml_buffers_holder.get())
, c_writers_holder(make_shared<decltype(c_writers_holder)::element_type>())
, c_writers(*c_writers_holder.get())
, clock_holder(make_shared<FreezingClock>())
, clock(*clock_holder.get())
, shard(block_device(device_holder), options.cluster_size, options.alignment,
        seastar::make_shared<metadata_log::to_disk_buffer_mocker>(ml_buffers_holder.get()),
        seastar::make_shared<cluster_writer_mocker>(c_writers_holder.get()), clock_holder)
{
    bootstrap_shard();
}

void shard_tester::bootstrap_shard(backend::shard& dest_shard) {
    dest_shard.bootstrap(options.root_dir, options.first_metadata_cluster_id, options.available_clusters, options.fs_shards_pool_size, options.fs_shard_id).get();
}

} // namespace seastar::fs::backend
