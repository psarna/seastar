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

#include "fs/cluster.hh"
#include "fs/inode.hh"
#include "fs/metadata_log.hh"
#include "seastar/fs/units.hh"
#include "seastar/core/do_with.hh"
#include "seastar/core/future.hh"
#include "seastar/core/temporary_buffer.hh"

#include <cstdint>
#include <optional>
#include <vector>

namespace seastar::fs {

class data_compaction {
    struct compacted_data_vec {
        temporary_buffer<uint8_t> _data;
        inode_t _inode_id;
        file_offset_t _file_offset;
        disk_offset_t _prev_disk_offset; // ondisk data position before the compaction
        std::optional<disk_offset_t> _post_disk_offset; // ondisk data position after the compaction
                                                        // std::nullopt if data is in memory
    };

    metadata_log& _metadata_log;
    std::vector<cluster_id_t> _compacted_cluster_ids;
    bool _was_metadata_log_modified = false; // if log was modified than we need to flush it before releasing clusters

    data_compaction(metadata_log& metadata_log, std::vector<cluster_id_t> compacted_cluster_ids)
        : _metadata_log(metadata_log), _compacted_cluster_ids(compacted_cluster_ids) {}

    future<> compact();

    future<> read_data_vectors(std::vector<compacted_data_vec> data_vecs);

    future<> group_data_into_clusters(std::vector<compacted_data_vec> read_data_vecs);

    future<> allocate_clusters(std::vector<std::vector<compacted_data_vec>> grouped_data_vecs,
            std::vector<compacted_data_vec> memory_data_vecs);

    future<> save_compacted_data_vecs(std::vector<cluster_id_t> comp_clusters_ids,
            std::vector<std::vector<compacted_data_vec>> grouped_data_vecs,
            std::vector<compacted_data_vec> memory_data_vecs);

    future<> write_ondisk_data_vecs(std::vector<compacted_data_vec> file_data_vecs);

    future<> write_memory_data_vecs(std::vector<compacted_data_vec> file_data_vecs);

    void update_previous_data_vecs(compacted_data_vec& vec);

public:
    static future<> perform(metadata_log& metadata_log, std::vector<cluster_id_t> cluster_ids) {
        for (auto& cluster_id : cluster_ids) {
            metadata_log.make_data_cluster_writable(cluster_id);
        }
        return do_with(data_compaction(metadata_log, std::move(cluster_ids)), [](auto& obj) {
            return obj.compact();
        });
    }
};

} // namespace seastar::fs
