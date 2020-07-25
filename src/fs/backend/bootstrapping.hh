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

#include "seastar/core/future.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/fs/unit_types.hh"

#include <optional>
#include <string_view>
#include <unordered_set>

namespace seastar::fs::backend::metadata_log::entries {
struct delete_inode;
} // namespace seastar::fs::backend::metadata_log::entries

namespace seastar::fs::backend {

class shard;

class bootstrapping {
    shard& _shard;
    const cluster_range _available_clusters;
    std::unordered_set<cluster_id_t> _metadata_log_clusters;

    std::optional<cluster_id_t> _next_cluster;

    struct curr_cluster {
        cluster_id_t id;
        temporary_buffer<uint8_t> content;
        std::string_view unparsed_content;
        std::string_view curr_checkpoint;
    } _curr_cluster;

public:
    static future<> bootstrap(shard& shard, inode_t root_dir, cluster_id_t first_metadata_cluster_id,
            cluster_range available_clusters, fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id);

private:
    bootstrapping(shard& shard, cluster_range available_clusters);

    future<> bootstrap(cluster_id_t first_metadata_cluster_id, fs_shard_id_t shards_pool_size,
            fs_shard_id_t shard_id);

    void initialize_shard_metadata_log_cbuf();

    enum [[nodiscard]] ca_init_res {
        SUCCESS,
        METADATA_AND_DATA_LOG_OVERLAP,
        NOSPACE
    };

    ca_init_res initialize_shard_cluster_allocator();

    void initialize_shard_inode_allocator(fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id);

    future<> read_curr_cluster();
    // Bootstraps already read _curr_cluster
    future<> bootstrap_curr_cluster();

    template<class Entry>
    future<> bootstrap_entry(Entry& entry);

    bool inode_exists(inode_t inode) const noexcept;

    future<> bootstrap_checkpointed_data();
};

} // namespace seastar::fs::backend
