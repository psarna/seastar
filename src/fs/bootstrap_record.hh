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
 * Copyright (C) 2019 ScyllaDB
 */

#pragma once

#include "fs/cluster.hh"
#include "fs/inode.hh"
#include "seastar/fs/block_device.hh"

#include <exception>

namespace seastar::fs {

class invalid_bootstrap_record : public std::runtime_error {
public:
    explicit invalid_bootstrap_record(const std::string& msg) : std::runtime_error(msg) {}
    explicit invalid_bootstrap_record(const char* msg) : std::runtime_error(msg) {}
};

/// In-memory version of the record describing characteristics of the file system (~superblock).
class bootstrap_record {
public:
    static constexpr uint64_t magic_number = 0x5343594c4c414653; // SCYLLAFS
    static constexpr uint32_t max_shards_nb = 500;
    static constexpr unit_size_t min_alignment = 4096;

    struct shard_info {
        cluster_id_t metadata_cluster; /// cluster id of the first metadata log cluster
        cluster_range available_clusters; /// range of clusters for data for this shard
    };

    uint64_t version; /// file system version
    unit_size_t alignment; /// write alignment in bytes
    unit_size_t cluster_size; /// cluster size in bytes
    inode_t root_directory; /// root dir inode number
    std::vector<shard_info> shards_info; /// basic informations about each file system shard

    bootstrap_record() = default;
    bootstrap_record(uint64_t version, unit_size_t alignment, unit_size_t cluster_size, inode_t root_directory,
            std::vector<shard_info> shards_info)
        : version(version), alignment(alignment), cluster_size(cluster_size) , root_directory(root_directory)
        , shards_info(std::move(shards_info)) {}

    /// number of file system shards
    uint32_t shards_nb() const noexcept {
        return shards_info.size();
    }

    static future<bootstrap_record> read_from_disk(block_device& device);
    future<> write_to_disk(block_device& device) const;

    friend bool operator==(const bootstrap_record&, const bootstrap_record&) noexcept;
    friend bool operator!=(const bootstrap_record&, const bootstrap_record&) noexcept;
};

inline bool operator==(const bootstrap_record::shard_info& lhs, const bootstrap_record::shard_info& rhs) noexcept {
    return lhs.metadata_cluster == rhs.metadata_cluster and lhs.available_clusters == rhs.available_clusters;
}

inline bool operator!=(const bootstrap_record::shard_info& lhs, const bootstrap_record::shard_info& rhs) noexcept {
    return !(lhs == rhs);
}

inline bool operator!=(const bootstrap_record& lhs, const bootstrap_record& rhs) noexcept {
    return !(lhs == rhs);
}

/// On-disk version of the record describing characteristics of the file system (~superblock).
struct bootstrap_record_disk {
    uint64_t magic;
    uint64_t version;
    unit_size_t alignment;
    unit_size_t cluster_size;
    inode_t root_directory;
    uint32_t shards_nb;
    bootstrap_record::shard_info shards_info[bootstrap_record::max_shards_nb];
    uint32_t crc;
};

}
