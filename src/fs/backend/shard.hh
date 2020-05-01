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
#include "fs/backend/cluster_writer.hh"
#include "fs/backend/inode_info.hh"
#include "fs/backend/metadata_log/to_disk_buffer.hh"
#include "fs/clock.hh"
#include "fs/inode_utils.hh"
#include "fs/value_shared_lock.hh"
#include "seastar/core/shared_future.hh"
#include "seastar/fs/exceptions.hh"
#include "seastar/fs/stat.hh"
#include "seastar/fs/unit_types.hh"

namespace seastar::fs::backend {

class shard {
    block_device _device;
    const disk_offset_t _cluster_size;
    const disk_offset_t _alignment;

    shared_future<> _background_futures = now(); // Background tasks

    // Takes care of writing metadata log entries to current metadata log cluster on device
    shared_ptr<metadata_log::to_disk_buffer> _metadata_log_cbuf;
    // Takes care of writing medium writes to current medium data log cluster on device
    shared_ptr<cluster_writer> _medium_data_log_cw;

    cluster_allocator _cluster_allocator; // Manages free clusters
    shard_inode_allocator _inode_allocator; // Supplies new inode numbers

    // Memory representation of fs metadata
    inode_t _root_dir;
    std::map<inode_t, inode_info> _inodes; // TODO: try using std::unordered_map

    shared_ptr<Clock> _clock;

    struct read_only_fs_tag { };
    using read_only_fs = bool_class<read_only_fs_tag>;

    read_only_fs _read_only_fs = read_only_fs::no;

    void throw_if_read_only_fs();

    void set_fs_read_only_mode(read_only_fs val) noexcept;

    // Locks are used to ensure metadata consistency while allowing concurrent usage.
    //
    // Whenever one wants to create or delete inode or directory entry, one has to acquire appropriate unique lock for
    // the inode / dir entry that will appear / disappear and only after locking that operation should take place.
    // Shared locks should be used only to ensure that an inode / dir entry won't disappear / appear, while some action
    // is performed. Therefore, unique locks ensure that resource is not used by anyone else.
    //
    // IMPORTANT: if an operation needs to acquire more than one lock, it has to be done with *one* call to
    //   locks::with_locks() because it is ensured there that a deadlock-free locking order is used (for details see
    //   that function).
    //
    // Examples:
    // - To create file we have to take shared lock (SL) on the directory to which we add a dir entry and
    //   unique lock (UL) on the added entry in this directory. SL is taken because the directory should not disappear.
    //   UL is taken, because we do not want the entry to appear while we are creating it.
    // - To read or write to a file, a SL is acquired on its inode and then the operation is performed.
    class locks {
        value_shared_lock<inode_t> _inode_locks;
        value_shared_lock<std::pair<inode_t, std::string>> _dir_entry_locks;

    public:
        struct shared {
            inode_t inode;
            std::optional<std::string> dentry;
        };

        template<class T>
        static constexpr bool is_shared = std::is_same_v<std::remove_cv_t<std::remove_reference_t<T>>, shared>;

        struct unique {
            inode_t inode;
            std::optional<std::string> dentry;
        };

        template<class T>
        static constexpr bool is_unique = std::is_same_v<std::remove_cv_t<std::remove_reference_t<T>>, unique>;

        template<class Kind, class Func> // TODO: use noncopyable_function
        auto with_lock(Kind kind, Func&& func) {
            static_assert(is_shared<Kind> || is_unique<Kind>);
            if constexpr (is_shared<Kind>) {
                if (kind.dentry.has_value()) {
                    return _dir_entry_locks.with_shared_on({kind.inode, std::move(*kind.dentry)},
                            std::forward<Func>(func));
                } else {
                    return _inode_locks.with_shared_on(kind.inode, std::forward<Func>(func));
                }
            } else {
                if (kind.dentry.has_value()) {
                    return _dir_entry_locks.with_lock_on({kind.inode, std::move(*kind.dentry)},
                            std::forward<Func>(func));
                } else {
                    return _inode_locks.with_lock_on(kind.inode, std::forward<Func>(func));
                }
            }
        }

    private:
        template<class Kind1, class Kind2, class Func> // TODO: use noncopyable_function
        auto with_locks_in_order(Kind1 kind1, Kind2 kind2, Func func) {
            // Func is not an universal reference because we will have to store it
            return with_lock(std::move(kind1), [this, kind2 = std::move(kind2), func = std::move(func)] () mutable {
                return with_lock(std::move(kind2), std::move(func));
            });
        };

    public:

        template<class Kind1, class Kind2, class Func> // TODO: use noncopyable_function
        auto with_locks(Kind1 kind1, Kind2 kind2, Func&& func) {
            static_assert(is_shared<Kind1> || is_unique<Kind1>);
            static_assert(is_shared<Kind2> || is_unique<Kind2>);

            // Locking order is as follows: kind with lower tuple (inode, dentry) goes first.
            // This order is linear and we always lock in one direction, so the graph of locking relations (A -> B iff
            // lock on A is acquired and lock on B is acquired / being acquired) makes a DAG. Thus, deadlock is
            // impossible, as it would require a cycle to appear.
            std::pair<inode_t, std::optional<std::string>&> k1 = {kind1.inode, kind1.dentry};
            std::pair<inode_t, std::optional<std::string>&> k2 = {kind2.inode, kind2.dentry};
            if (k1 < k2) {
                return with_locks_in_order(std::move(kind1), std::move(kind2), std::forward<Func>(func));
            } else {
                return with_locks_in_order(std::move(kind2), std::move(kind1), std::forward<Func>(func));
            }
        }
    } _locks;

    // TODO: for compaction: keep some set(?) of inode_data_vec, so that we can keep track of clusters that have lowest
    //       utilization (up-to-date data)
    // TODO: for compaction: keep estimated metadata log size (that would take when written to disk) and
    //       the real size of metadata log taken on disk to allow for detecting when compaction

    friend class bootstrapping;

    friend class create_and_open_unlinked_file_operation;
    friend class create_file_operation;
    friend class link_file_operation;
    friend class read_operation;
    friend class truncate_operation;
    friend class unlink_or_remove_file_operation;
    friend class write_operation;

public:
    shard(block_device device, disk_offset_t cluster_size, disk_offset_t alignment,
            shared_ptr<metadata_log::to_disk_buffer> metadata_log_cbuf, shared_ptr<cluster_writer> medium_data_log_cw, shared_ptr<Clock> clock);

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

    void write_update(inode_info::file& file, inode_data_vec new_data_vec);

    // Deletes data vectors that are subset of @p data_range and cuts overlapping data vectors to make them not overlap
    void cut_out_data_range(inode_info::file& file, file_range range);

    inode_info& memory_only_create_inode(inode_t inode, unix_metadata metadata);
    void memory_only_delete_inode(inode_t inode);
    void memory_only_small_write(inode_t inode, disk_offset_t offset, temporary_buffer<uint8_t> data);
    void memory_only_disk_write(inode_t inode, file_offset_t file_offset, disk_offset_t disk_offset, size_t write_len);
    void memory_only_update_mtime(inode_t inode, decltype(unix_metadata::mtime_ns) mtime_ns);
    void memory_only_truncate(inode_t inode, disk_offset_t size);
    void memory_only_create_dentry(inode_info::directory& dir, inode_t entry_inode, std::string entry_name);
    void memory_only_delete_dentry(inode_info::directory& dir, const std::string& entry_name);

    template<class Func> // TODO: use noncopyable_function
    void schedule_background_task(Func&& task) {
        // FIXME: bound concurrency, but be careful in the caller of this function, as now following assumption is taken
        //        in the caller: scheduling background task is atomic and not asynchronous, thus no locking is required
        //        on resources because no other continuation will be scheduled
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
        throw_if_read_only_fs();

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

    void schedule_attempt_to_delete_inode(inode_t inode);

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

    fs::stat_data stat(inode_t inode) const;

    fs::stat_data stat(const std::string& path) const;

    // Returns size of the file or throws exception iff @p inode is invalid
    file_offset_t file_size(inode_t inode) const;

    future<> create_file(std::string path, file_permissions perms);

    future<inode_t> create_and_open_file(std::string path, file_permissions perms);

    future<inode_t> create_and_open_unlinked_file(file_permissions perms);

    future<> create_directory(std::string path, file_permissions perms);

    // Creates name (@p path) for a file (@p inode)
    future<> link_file(inode_t inode, std::string path);

    // Creates name (@p destination) for a file (not directory) @p source
    future<> link_file(std::string source, std::string destination);

    future<> unlink_file(std::string path);

    future<> remove_directory(std::string path);

    // Removes empty directory or unlinks file
    future<> remove(std::string path);

    // TODO: what about permissions, uid, gid etc.
    future<inode_t> open_file(std::string path);

    future<> close_file(inode_t inode);

    // Unaligned reads and writes are supported but discouraged because of bad performance impact
    future<size_t> read(inode_t inode, file_offset_t pos, void* buffer, size_t len,
            const io_priority_class& pc = default_priority_class());

    future<size_t> write(inode_t inode, file_offset_t pos, const void* buffer, size_t len,
            const io_priority_class& pc = default_priority_class());

    // Truncates a file or extends it with a "hole" data_vec to a specified size
    future<> truncate(inode_t inode, file_offset_t size);

    // All disk-related errors will be exposed here
    future<> flush_log() {
        return flush_curr_cluster();
    }

    // Returns approximation of available space for storing data
    size_t remaining_space() const noexcept {
        return _cluster_allocator.remaining_clusters_number() * _cluster_size;
    }
};

} // namespace seastar::fs::backend
