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

#include "fs/backend/bootstrapping.hh"
#include "fs/backend/create_and_open_unlinked_file.hh"
#include "fs/backend/create_file.hh"
#include "fs/backend/inode_info.hh"
#include "fs/backend/link_file.hh"
#include "fs/backend/metadata_log/entries.hh"
#include "fs/backend/read.hh"
#include "fs/backend/shard.hh"
#include "fs/backend/unlink_or_remove_file.hh"
#include "fs/backend/write.hh"
#include "fs/cluster_utils.hh"
#include "fs/unix_metadata.hh"
#include "seastar/core/thread.hh"

namespace mle = seastar::fs::backend::metadata_log::entries;

namespace {
seastar::logger mlogger("fs_backend_shard");
} // namespace

namespace seastar::fs::backend {

shard::shard(block_device device, disk_offset_t cluster_size, disk_offset_t alignment,
    shared_ptr<metadata_log::to_disk_buffer> metadata_log_cbuf, shared_ptr<cluster_writer> medium_data_log_cw,
    shared_ptr<Clock> clock)
: _device(std::move(device))
, _cluster_size(cluster_size)
, _alignment(alignment)
, _metadata_log_cbuf(std::move(metadata_log_cbuf))
, _medium_data_log_cw(std::move(medium_data_log_cw))
, _cluster_allocator({}, {})
, _inode_allocator(1, 0)
, _clock(std::move(clock)) {
    assert(is_power_of_2(alignment));
    assert(cluster_size > 0 && cluster_size % alignment == 0);
}

shard::shard(block_device device, disk_offset_t cluster_size, disk_offset_t alignment)
: shard(std::move(device), cluster_size, alignment,
        make_shared<metadata_log::to_disk_buffer>(), make_shared<cluster_writer>(), make_shared<Clock>()) {}

future<> shard::bootstrap(inode_t root_dir, cluster_id_t first_metadata_cluster_id, cluster_range available_clusters,
        fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id) {
    return bootstrapping::bootstrap(*this, root_dir, first_metadata_cluster_id, available_clusters, fs_shards_pool_size,
            fs_shard_id);
}

future<> shard::shutdown() {
    return async([this] {
        // TODO: Wait for all active operations (reads, writes, etc.) and don't
        //       allow for any new operations
        try {
            flush_log().get();
        } catch (...) {
            mlogger.warn("Error while flushing log during shutdown: {}", std::current_exception());
        }
        _device.close().get();
    });
}

void shard::write_update(inode_info::file& file, inode_data_vec new_data_vec) {
    // TODO: for compaction: update used inode_data_vec
    auto file_size = file.size();
    if (file_size < new_data_vec.data_range.beg) {
        file.data.emplace(file_size, inode_data_vec{
            .data_range = {
                .beg = file_size,
                .end = new_data_vec.data_range.beg,
            },
            .data_location = inode_data_vec::hole_data{},
        });
    } else {
        cut_out_data_range(file, new_data_vec.data_range);
    }

    file.data.emplace(new_data_vec.data_range.beg, std::move(new_data_vec));
}

void shard::cut_out_data_range(inode_info::file& file, file_range range) {
    file.cut_out_data_range(range, [](inode_data_vec data_vec) {
        (void)data_vec; // TODO: for compaction: update used inode_data_vec
    });
}

inode_info& shard::memory_only_create_inode(inode_t inode, unix_metadata metadata) {
    assert(_inodes.count(inode) == 0);
    return _inodes.emplace(inode, inode_info{
        .opened_files_count = 0,
        .links_count = 0,
        .metadata = metadata,
        .contents = [&]() -> decltype(inode_info::contents) {
            switch (metadata.ftype) {
            case file_type::DIRECTORY: return inode_info::directory{};
            case file_type::REGULAR_FILE: return inode_info::file{};
            }

            assert(false); // Invalid file_type
        }()
    }).first->second;
}

void shard::memory_only_delete_inode(inode_t inode) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(!it->second.is_open());
    assert(!it->second.is_linked());

    std::visit(overloaded{
        [](const inode_info::directory& dir) {
            assert(dir.entries.empty());
        },
        [](const inode_info::file&) {
            // TODO: for compaction: update used inode_data_vec
        }
    }, it->second.contents);

    _inodes.erase(it);
}

void shard::memory_only_small_write(inode_t inode, file_offset_t file_offset, temporary_buffer<uint8_t> data) {
    inode_data_vec data_vec = {
        .data_range = {
            .beg = file_offset,
            .end = file_offset + data.size(),
        },
        .data_location = inode_data_vec::in_mem_data{std::move(data)},
    };

    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(it->second.is_file());
    write_update(it->second.get_file(), std::move(data_vec));
}

void shard::memory_only_disk_write(inode_t inode, file_offset_t file_offset, disk_offset_t disk_offset,
        size_t write_len) {
    inode_data_vec data_vec = {
        .data_range = {
            .beg = file_offset,
            .end = file_offset + write_len,
        },
        .data_location = inode_data_vec::on_disk_data{
            .device_offset = disk_offset
        },
    };

    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(it->second.is_file());
    write_update(it->second.get_file(), std::move(data_vec));
}

void shard::memory_only_update_mtime(inode_t inode, decltype(unix_metadata::mtime_ns) mtime_ns) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    it->second.metadata.mtime_ns = mtime_ns;
    // ctime should be updated when contents is modified
    if (it->second.metadata.ctime_ns < mtime_ns) {
        it->second.metadata.ctime_ns = mtime_ns;
    }
}

void shard::memory_only_create_dentry(inode_info::directory& dir, inode_t entry_inode, std::string entry_name) {
    auto it = _inodes.find(entry_inode);
    assert(it != _inodes.end());
    // Directory may only be linked once (to avoid creating cycles)
    assert(!it->second.is_directory() || !it->second.is_linked());

    bool inserted = dir.entries.emplace(std::move(entry_name), entry_inode).second;
    assert(inserted);
    ++it->second.links_count;
}

void shard::memory_only_delete_dentry(inode_info::directory& dir, const std::string& entry_name) {
    auto it = dir.entries.find(entry_name);
    assert(it != dir.entries.end());

    auto entry_it = _inodes.find(it->second);
    assert(entry_it != _inodes.end());
    assert(entry_it->second.is_linked());

    --entry_it->second.links_count;
    dir.entries.erase(it);
}

void shard::schedule_flush_of_curr_cluster() {
    // Make writes concurrent (TODO: maybe serialized within *one* cluster would be faster?)
    schedule_background_task(do_with(_metadata_log_cbuf, &_device, [](auto& crr_clstr_bf, auto& device) {
        return crr_clstr_bf->flush_to_disk(*device);
    }));
}

future<> shard::flush_curr_cluster() {
    if (_metadata_log_cbuf->bytes_left_after_flush_if_done_now() == 0) {
        switch (schedule_flush_of_curr_cluster_and_change_it_to_new_one()) {
        case flush_result::NO_SPACE:
            return make_exception_future(no_more_space_exception());
        case flush_result::DONE:
            break;
        }
    } else {
        schedule_flush_of_curr_cluster();
    }

    return _background_futures.get_future();
}

shard::flush_result shard::schedule_flush_of_curr_cluster_and_change_it_to_new_one() {
    auto next_cluster = _cluster_allocator.alloc();
    if (!next_cluster) {
        // Here shard dies, we cannot even flush current cluster because from there we won't be able to recover
        // TODO: ^ add protection from it and take it into account during compaction
        return flush_result::NO_SPACE;
    }

    auto append_res = _metadata_log_cbuf->append(mle::next_metadata_cluster{
        .cluster_id = *next_cluster
    });
    assert(append_res == metadata_log::to_disk_buffer::APPENDED);
    schedule_flush_of_curr_cluster();

    // Make next cluster the current cluster to allow writing next metadata entries before flushing finishes
    _metadata_log_cbuf = _metadata_log_cbuf->virtual_constructor();
    _metadata_log_cbuf->init(_cluster_size, _alignment,
            cluster_id_to_offset(*next_cluster, _cluster_size));
    return flush_result::DONE;
}

void shard::schedule_attempt_to_delete_inode(inode_t inode) {
    return schedule_background_task([this, inode] {
        auto it = _inodes.find(inode);
        if (it == _inodes.end() || it->second.is_linked() || it->second.is_open()) {
            return now(); // Scheduled delete became invalid
        }

        switch (append_metadata_log(mle::delete_inode{.inode = inode})) {
        case append_result::TOO_BIG:
            assert(false && "ondisk entry cannot be too big");
        case append_result::NO_SPACE:
            return make_exception_future(no_more_space_exception());
        case append_result::APPENDED:
            memory_only_delete_inode(inode);
            return now();
        }
        __builtin_unreachable();
    });
}

std::variant<inode_t, shard::path_lookup_error> shard::do_path_lookup(const std::string& path) const noexcept {
    if (path.empty() || path[0] != '/') {
        return path_lookup_error::NOT_ABSOLUTE;
    }

    std::vector<inode_t> components_stack = {_root_dir};
    size_t beg = 0;
    while (beg < path.size()) {
        range<size_t> component_range = {
            .beg = beg,
            .end = path.find('/', beg),
        };
        bool check_if_dir = false;
        if (component_range.end == path.npos) {
            component_range.end = path.size();
            beg = path.size();
        } else {
            check_if_dir = true;
            beg = component_range.end + 1; // Jump over '/'
        }

        std::string_view component(path.data() + component_range.beg, component_range.size());
        // Process the component
        if (component == "") {
            continue;
        } else if (component == ".") {
            assert(component_range.beg > 0 && path[component_range.beg - 1] == '/' && "Since path is absolute we do not have to check if the current component is a directory");
            continue;
        } else if (component == "..") {
            if (components_stack.size() > 1) { // Root dir cannot be popped
                components_stack.pop_back();
            }
        } else {
            auto dir_it = _inodes.find(components_stack.back());
            assert(dir_it != _inodes.end() && "inode comes from some previous lookup (or is a root directory) hence dir_it has to be valid");
            assert(dir_it->second.is_directory() && "every previous component is a directory and it was checked when they were processed");
            auto& curr_dir = dir_it->second.get_directory();

            auto it = curr_dir.entries.find(component);
            if (it == curr_dir.entries.end()) {
                return path_lookup_error::NO_ENTRY;
            }

            inode_t entry_inode = it->second;
            if (check_if_dir) {
                auto entry_it = _inodes.find(entry_inode);
                assert(entry_it != _inodes.end() && "dir entries have to exist");
                if (!entry_it->second.is_directory()) {
                    return path_lookup_error::NOT_DIR;
                }
            }

            components_stack.emplace_back(entry_inode);
        }
    }

    return components_stack.back();
}

future<inode_t> shard::path_lookup(const std::string& path) const {
    return std::visit(overloaded{
        [](path_lookup_error error) {
            switch (error) {
            case path_lookup_error::NOT_ABSOLUTE:
                return make_exception_future<inode_t>(path_is_not_absolute_exception());
            case path_lookup_error::NO_ENTRY:
                return make_exception_future<inode_t>(no_such_file_or_directory_exception());
            case path_lookup_error::NOT_DIR:
                return make_exception_future<inode_t>(path_component_not_directory_exception());
            }
            __builtin_unreachable();
        },
        [](inode_t inode) {
            return make_ready_future<inode_t>(inode);
        }
    }, do_path_lookup(path));
}

file_offset_t shard::file_size(inode_t inode) const {
    auto it = _inodes.find(inode);
    if (it == _inodes.end()) {
        throw invalid_inode_exception();
    }

    return std::visit(overloaded{
        [](const inode_info::file& file) {
            return file.size();
        },
        [](const inode_info::directory&) -> file_offset_t {
            throw invalid_inode_exception();
        }
    }, it->second.contents);
}

future<> shard::create_file(std::string path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), create_semantics::CREATE_FILE).discard_result();
}

future<inode_t> shard::create_and_open_file(std::string path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), create_semantics::CREATE_AND_OPEN_FILE);
}

future<inode_t> shard::create_and_open_unlinked_file(file_permissions perms) {
    return create_and_open_unlinked_file_operation::perform(*this, std::move(perms));
}

future<> shard::create_directory(std::string path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), create_semantics::CREATE_DIR).discard_result();
}

future<> shard::link_file(inode_t inode, std::string path) {
    return link_file_operation::perform(*this, inode, std::move(path));
}

future<> shard::link_file(std::string source, std::string destination) {
    return path_lookup(std::move(source)).then([this, destination = std::move(destination)](inode_t inode) {
        return link_file(inode, std::move(destination));
    });
}

future<> shard::unlink_file(std::string path) {
    return unlink_or_remove_file_operation::perform(*this, std::move(path), remove_semantics::FILE_ONLY);
}

future<> shard::remove_directory(std::string path) {
    return unlink_or_remove_file_operation::perform(*this, std::move(path), remove_semantics::DIR_ONLY);
}

future<> shard::remove(std::string path) {
    return unlink_or_remove_file_operation::perform(*this, std::move(path), remove_semantics::FILE_OR_DIR);
}

future<inode_t> shard::open_file(std::string path) {
    return path_lookup(path).then([this](inode_t inode) {
        auto inode_it = _inodes.find(inode);
        if (inode_it == _inodes.end()) {
            return make_exception_future<inode_t>(operation_became_invalid_exception());
        }
        inode_info* inode_info = &inode_it->second;
        if (inode_info->is_directory()) {
            return make_exception_future<inode_t>(is_directory_exception());
        }

        // TODO: can be replaced by sth like _inode_info.during_delete
        return _locks.with_lock(shard::locks::shared{.inode = inode}, [this, inode_info = std::move(inode_info), inode] {
            if (!inode_exists(inode)) {
                return make_exception_future<inode_t>(operation_became_invalid_exception());
            }
            ++inode_info->opened_files_count;
            return make_ready_future<inode_t>(inode);
        });
    });
}

future<> shard::close_file(inode_t inode) {
    auto inode_it = _inodes.find(inode);
    if (inode_it == _inodes.end()) {
        return make_exception_future(invalid_inode_exception());
    }
    inode_info* inode_info = &inode_it->second;
    if (inode_info->is_directory()) {
        return make_exception_future(is_directory_exception());
    }


    return _locks.with_lock(shard::locks::shared{.inode = inode}, [this, inode, inode_info] {
        if (!inode_exists(inode)) {
            return make_exception_future(operation_became_invalid_exception());
        }

        assert(inode_info->is_open());

        --inode_info->opened_files_count;
        if (!inode_info->is_linked() && !inode_info->is_open()) {
            // Unlinked and not open file should be removed
            schedule_attempt_to_delete_inode(inode);
        }
        return now();
    });
}

future<size_t> shard::read(inode_t inode, file_offset_t pos, void* buffer, size_t len,
        const io_priority_class& pc) {
    return read_operation::perform(*this, inode, pos, buffer, len, pc);
}

future<size_t> shard::write(inode_t inode, file_offset_t pos, const void* buffer, size_t len,
        const io_priority_class& pc) {
    return write_operation::perform(*this, inode, pos, buffer, len, pc);
}

// TODO: think about how to make filesystem recoverable from ENOSPACE situation: flush() (or something else) throws ENOSPACE,
// then it should be possible to compact some data (e.g. by truncating a file) via top-level interface and retrying the flush()
// without a ENOSPACE error. In particular if we delete all files after ENOSPACE it should be successful. It becomes especially
// hard if we write metadata to the last cluster and there is no enough room to write these delete operations. We have to
// guarantee that the filesystem is in a recoverable state then.

} // namespace seastar::fs::backend
