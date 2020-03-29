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

#include "fs/bitwise.hh"
#include "fs/cluster.hh"
#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_to_disk_buffer.hh"
#include "fs/units.hh"
#include "fs/metadata_log.hh"
#include "seastar/core/do_with.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/core/temporary_buffer.hh"

#include <boost/crc.hpp>
#include <cstddef>
#include <cstring>
#include <unordered_set>
#include <variant>

namespace seastar::fs {

// TODO: add a comment about what it is
class data_reader {
    const uint8_t* _data = nullptr;
    size_t _size = 0;
    size_t _pos = 0;
    size_t _last_checkpointed_pos = 0;

public:
    data_reader() = default;

    data_reader(const uint8_t* data, size_t size) : _data(data), _size(size) {}

    size_t curr_pos() const noexcept { return _pos; }

    size_t last_checkpointed_pos() const noexcept { return _last_checkpointed_pos; }

    size_t bytes_left() const noexcept { return _size - _pos; }

    void align_curr_pos(size_t alignment) noexcept { _pos = round_up_to_multiple_of_power_of_2(_pos, alignment); }

    void checkpoint_curr_pos() noexcept { _last_checkpointed_pos = _pos; }

    // Returns whether the reading was successful
    bool read(void* destination, size_t size);

    // Returns whether the reading was successful
    template<class T>
    bool read_entry(T& entry) noexcept {
        return read(&entry, sizeof(entry));
    }

    // Returns whether the reading was successful
    bool read_string(std::string& str, size_t size);

    std::optional<temporary_buffer<uint8_t>> read_tmp_buff(size_t size);

    // Returns whether the processing was successful
    bool process_crc_without_reading(boost::crc_32_type& crc, size_t size);

    std::optional<data_reader> extract(size_t size);
};

class metadata_log_bootstrap {
    metadata_log& _metadata_log;
    cluster_range _available_clusters;
    std::unordered_set<cluster_id_t> _taken_clusters;
    std::optional<cluster_id_t> _next_cluster;
    temporary_buffer<uint8_t> _curr_cluster_data;
    data_reader _curr_cluster;
    data_reader _curr_checkpoint;

    metadata_log_bootstrap(metadata_log& metadata_log, cluster_range available_clusters);

    future<> bootstrap(cluster_id_t first_metadata_cluster_id, fs_shard_id_t fs_shards_pool_size,
            fs_shard_id_t fs_shard_id);

    future<> bootstrap_cluster(cluster_id_t curr_cluster);

    static auto invalid_entry_exception() {
        return make_exception_future<>(std::runtime_error("Invalid metadata log entry"));
    }

    future<> bootstrap_read_cluster();

    // Returns whether reading and checking was successful
    bool read_and_check_checkpoint();

    future<> bootstrap_checkpointed_data();

    future<> bootstrap_next_metadata_cluster();

    bool inode_exists(inode_t inode);

    future<> bootstrap_create_inode();

    future<> bootstrap_delete_inode();

    future<> bootstrap_create_inode_as_dir_entry();

public:
    static future<> bootstrap(metadata_log& metadata_log, inode_t root_dir, cluster_id_t first_metadata_cluster_id,
            cluster_range available_clusters, fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id);
};

} // namespace seastar::fs
