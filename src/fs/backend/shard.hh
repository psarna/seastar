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

#include "fs/backend/cluster_allocator.hh"
#include "fs/backend/inode_info.hh"
#include "fs/backend/metadata_log/to_disk_buffer.hh"
#include "fs/clock.hh"
#include "fs/inode_utils.hh"
#include "seastar/core/shared_future.hh"
#include "seastar/fs/exceptions.hh"
#include "seastar/fs/unit_types.hh"

namespace seastar::fs::backend {

class shard {
    block_device _device;
    const disk_offset_t _cluster_size;
    const disk_offset_t _alignment;

    shared_future<> _background_futures = now(); // Background tasks

    // Takes care of writing metadata log entries to current metadata log cluster on device
    shared_ptr<metadata_log::to_disk_buffer> _metadata_log_cbuf;

    cluster_allocator _cluster_allocator; // Manages free clusters
    shard_inode_allocator _inode_allocator; // Supplies new inode numbers

    // Memory representation of fs metadata
    inode_t _root_dir;
    std::map<inode_t, inode_info> _inodes; // TODO: try using std::unordered_map

    shared_ptr<Clock> _clock;

    // TODO: for compaction: keep some set(?) of inode_data_vec, so that we can keep track of clusters that have lowest
    //       utilization (up-to-date data)
    // TODO: for compaction: keep estimated metadata log size (that would take when written to disk) and
    //       the real size of metadata log taken on disk to allow for detecting when compaction

    friend class bootstrapping;

public:
    shard(block_device device, disk_offset_t cluster_size, disk_offset_t alignment,
            shared_ptr<metadata_log::to_disk_buffer> metadata_log_cbuf, shared_ptr<Clock> clock);

    shard(block_device device, disk_offset_t cluster_size, disk_offset_t alignment);

    shard(const shard&) = delete;
    shard& operator=(const shard&) = delete;
    shard(shard&&) = default;

    future<> bootstrap(inode_t root_dir, cluster_id_t first_metadata_cluster_id, cluster_range available_clusters,
            fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id);

    future<> shutdown();

private:
    bool inode_exists(inode_t inode) const noexcept {
        return _inodes.count(inode) != 0;
    }

    template<class Func>
    void schedule_background_task(Func&& task) {
        _background_futures = when_all_succeed(_background_futures.get_future(), std::forward<Func>(task));
    }

    void schedule_flush_of_curr_cluster();

    enum class flush_result {
        DONE,
        NO_SPACE
    };

    [[nodiscard]] flush_result schedule_flush_of_curr_cluster_and_change_it_to_new_one();

    future<> flush_curr_cluster();

    enum class append_result {
        APPENDED,
        TOO_BIG,
        NO_SPACE
    };

    template<class Entry>
    [[nodiscard]] append_result append_metadata_log(const Entry& entry) {
        using AR = append_result;
        // TODO: maybe check for errors on _background_futures to expose previous errors?
        switch (_metadata_log_cbuf->append(entry)) {
        case metadata_log::to_disk_buffer::APPENDED:
            return AR::APPENDED;
        case metadata_log::to_disk_buffer::TOO_BIG:
            break;
        }

        switch (schedule_flush_of_curr_cluster_and_change_it_to_new_one()) {
        case flush_result::NO_SPACE:
            return AR::NO_SPACE;
        case flush_result::DONE:
            break;
        }

        switch (_metadata_log_cbuf->append(entry)) {
        case metadata_log::to_disk_buffer::APPENDED:
            return AR::APPENDED;
        case metadata_log::to_disk_buffer::TOO_BIG:
            return AR::TOO_BIG;
        }

        __builtin_unreachable();
    }

    enum class path_lookup_error {
        NOT_ABSOLUTE, // a path is not absolute
        NO_ENTRY, // no such file or directory
        NOT_DIR, // a component used as a directory in path is not, in fact, a directory
    };

    std::variant<inode_t, path_lookup_error> do_path_lookup(const std::string& path) const noexcept;

    // It is safe for @p path to be a temporary (there is no need to worry about its lifetime)
    future<inode_t> path_lookup(const std::string& path) const;

public:
    template<class Func> // TODO: noncopyable_function
    future<> iterate_directory(const std::string& dir_path, Func func) const {
        static_assert(std::is_invocable_r_v<future<>, Func, const std::string&> ||
                std::is_invocable_r_v<future<stop_iteration>, Func, const std::string&>);
        auto convert_func = [&]() -> decltype(auto) {
            if constexpr (std::is_invocable_r_v<future<stop_iteration>, Func, const std::string&>) {
                return std::move(func);
            } else {
                return [func = std::move(func)]() -> future<stop_iteration> {
                    return func().then([] {
                        return stop_iteration::no;
                    });
                };
            }
        };
        return path_lookup(dir_path).then([this, func = convert_func()](inode_t dir_inode) {
            return do_with(std::move(func), std::string{}, [this, dir_inode](auto& func, auto& prev_entry) {
                auto it = _inodes.find(dir_inode);
                if (it == _inodes.end()) {
                    return now(); // Directory disappeared
                }
                if (!it->second.is_directory()) {
                    return make_exception_future(path_component_not_directory_exception());
                }

                return repeat([this, dir_inode, &prev_entry, &func] {
                    auto it = _inodes.find(dir_inode);
                    if (it == _inodes.end()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes); // Directory disappeared
                    }
                    assert(it->second.is_directory() && "Directory cannot become a file");
                    auto& dir = it->second.get_directory();

                    auto entry_it = dir.entries.upper_bound(prev_entry);
                    if (entry_it == dir.entries.end()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes); // No more entries
                    }

                    prev_entry = entry_it->first;
                    // Cast is here to prevent func taking a non-const reference if such overload exists
                    return func(static_cast<const std::string&>(prev_entry));
                });
            });
        });
    }

    // Returns size of the file or throws exception iff @p inode is invalid
    file_offset_t file_size(inode_t inode) const;

    // All disk-related errors will be exposed here
    future<> flush_log() {
        return flush_curr_cluster();
    }
};

} // namespace seastar::fs::backend
