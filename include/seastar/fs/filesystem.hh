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

#include "file_handle.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sharded.hh"
#include "seastar/core/shared_mutex.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/fs/shared_root.hh"
#include "seastar/fs/unit_types.hh"

namespace seastar::fs {

namespace backend {
class shard;
} // namespace backend

class shared_root;

class filesystem final : public peering_sharded_service<filesystem> {
    shared_ptr<backend::shard> _backend_shard;
    shared_root _foreign_root;
    shared_entries _cache_root;
    shared_mutex _lock; /* TODO use write-lock for remove and create, read-lock for open and list */
public:
    filesystem() = default;
    filesystem(const filesystem&) = delete;

    filesystem& operator=(const filesystem&) = delete;
    filesystem(filesystem&&) = default;

    future<> start(std::string device_path, foreign_ptr<lw_shared_ptr<global_shared_root>> root, double compactness,
        size_t max_data_compaction_memory);

    future<shared_entries> local_root();

    future<shared_entries> global_root();

    future<> update_cache();

    future<file> open_file_dma(std::string name, open_flags flags);

    future<> create_directory(std::string path);

    /* TODO: move logic to open_file with a creation using open_flags */
    future<file> create_and_open_file(std::string name,  open_flags flags);

    future<> create_file(std::string path, file_permissions perms);

    future<file> open_file(std::string name);

    future<> remove(std::string path);

    future<> stop();

    future<> flush();

    size_t remaining_space();

private:
    future<stub_file_handle> create_and_open_file_handler(std::string name);
    future<stub_file_handle> create_and_open_inode(std::string path);
};

future<> bootfs(sharded<filesystem>& fs, std::string device_path, double compactness, size_t max_data_compaction_memory);

future<> mkfs(std::string device_path, uint64_t version, disk_offset_t cluster_size, disk_offset_t alignment,
        inode_t root_directory, uint32_t shards_nb);

} // namespace seastar::fs
