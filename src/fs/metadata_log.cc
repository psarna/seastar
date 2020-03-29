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

#include "fs/cluster.hh"
#include "fs/cluster_allocator.hh"
#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "fs/metadata_log_bootstrap.hh"
#include "fs/metadata_log_operations/create_and_open_unlinked_file.hh"
#include "fs/metadata_log_operations/create_file.hh"
#include "fs/metadata_log_operations/link_file.hh"
#include "fs/metadata_log_operations/truncate.hh"
#include "fs/metadata_log_operations/unlink_or_remove_file.hh"
#include "fs/metadata_log_operations/write.hh"
#include "fs/metadata_to_disk_buffer.hh"
#include "fs/path.hh"
#include "fs/units.hh"
#include "fs/unix_metadata.hh"
#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/do_with.hh"
#include "seastar/core/file-types.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/core/shared_mutex.hh"
#include "seastar/fs/overloaded.hh"

#include <boost/crc.hpp>
#include <boost/range/irange.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <unordered_set>
#include <variant>

namespace seastar::fs {

metadata_log::metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment,
    shared_ptr<metadata_to_disk_buffer> cluster_buff, shared_ptr<cluster_writer> data_writer)
: _device(std::move(device))
, _cluster_size(cluster_size)
, _alignment(alignment)
, _curr_cluster_buff(std::move(cluster_buff))
, _curr_data_writer(std::move(data_writer))
, _cluster_allocator({}, {})
, _inode_allocator(1, 0) {
    assert(is_power_of_2(alignment));
    assert(cluster_size > 0 and cluster_size % alignment == 0);
}

metadata_log::metadata_log(block_device device, unit_size_t cluster_size, unit_size_t alignment)
: metadata_log(std::move(device), cluster_size, alignment,
        make_shared<metadata_to_disk_buffer>(), make_shared<cluster_writer>()) {}

future<> metadata_log::bootstrap(inode_t root_dir, cluster_id_t first_metadata_cluster_id, cluster_range available_clusters,
        fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id) {
    return metadata_log_bootstrap::bootstrap(*this, root_dir, first_metadata_cluster_id, available_clusters,
            fs_shards_pool_size, fs_shard_id);
}

future<> metadata_log::shutdown() {
    return flush_log().then([this] {
        return _device.close();
    });
}

void metadata_log::write_update(inode_info::file& file, inode_data_vec new_data_vec) {
    // TODO: for compaction: update used inode_data_vec
    auto file_size = file.size();
    if (file_size < new_data_vec.data_range.beg) {
        file.data.emplace(file_size, inode_data_vec {
            {file_size, new_data_vec.data_range.beg},
            inode_data_vec::hole_data {}
        });
    } else {
        cut_out_data_range(file, new_data_vec.data_range);
    }

    file.data.emplace(new_data_vec.data_range.beg, std::move(new_data_vec));
}

void metadata_log::cut_out_data_range(inode_info::file& file, file_range range) {
    file.cut_out_data_range(range, [](inode_data_vec data_vec) {
        (void)data_vec; // TODO: for compaction: update used inode_data_vec
    });
}

inode_info& metadata_log::memory_only_create_inode(inode_t inode, bool is_directory, unix_metadata metadata) {
    assert(_inodes.count(inode) == 0);
    return _inodes.emplace(inode, inode_info {
        0,
        0,
        metadata,
        [&]() -> decltype(inode_info::contents) {
            if (is_directory) {
                return inode_info::directory {};
            }

            return inode_info::file {};
        }()
    }).first->second;
}

void metadata_log::memory_only_delete_inode(inode_t inode) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(not it->second.is_open());
    assert(not it->second.is_linked());

    std::visit(overloaded {
        [](const inode_info::directory& dir) {
            assert(dir.entries.empty());
        },
        [](const inode_info::file&) {
            // TODO: for compaction: update used inode_data_vec
        }
    }, it->second.contents);

    _inodes.erase(it);
}

void metadata_log::memory_only_small_write(inode_t inode, file_offset_t file_offset, temporary_buffer<uint8_t> data) {
    inode_data_vec data_vec = {
        {file_offset, file_offset + data.size()},
        inode_data_vec::in_mem_data {std::move(data)}
    };

    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(it->second.is_file());
    write_update(it->second.get_file(), std::move(data_vec));
}

void metadata_log::memory_only_disk_write(inode_t inode, file_offset_t file_offset, disk_offset_t disk_offset,
        size_t write_len) {
    inode_data_vec data_vec = {
        {file_offset, file_offset + write_len},
        inode_data_vec::on_disk_data {disk_offset}
    };

    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(it->second.is_file());
    write_update(it->second.get_file(), std::move(data_vec));
}

void metadata_log::memory_only_update_mtime(inode_t inode, decltype(unix_metadata::mtime_ns) mtime_ns) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    it->second.metadata.mtime_ns = mtime_ns;
    // ctime should be updated when contents is modified
    if (it->second.metadata.ctime_ns < mtime_ns) {
        it->second.metadata.ctime_ns = mtime_ns;
    }
}

void metadata_log::memory_only_truncate(inode_t inode, file_offset_t size) {
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(it->second.is_file());
    auto& file = it->second.get_file();

    auto file_size = file.size();
    if (size > file_size) {
        file.data.emplace(file_size, inode_data_vec {
            {file_size, size},
            inode_data_vec::hole_data {}
        });
    } else {
        // TODO: for compaction: update used inode_data_vec
        cut_out_data_range(file, {
            size,
            std::numeric_limits<decltype(file_range::end)>::max()
        });
    }
}

void metadata_log::memory_only_add_dir_entry(inode_info::directory& dir, inode_t entry_inode, std::string entry_name) {
    auto it = _inodes.find(entry_inode);
    assert(it != _inodes.end());
    // Directory may only be linked once (to avoid creating cycles)
    assert(not it->second.is_directory() or not it->second.is_linked());

    bool inserted = dir.entries.emplace(std::move(entry_name), entry_inode).second;
    assert(inserted);
    ++it->second.directories_containing_file;
}

void metadata_log::memory_only_delete_dir_entry(inode_info::directory& dir, std::string entry_name) {
    auto it = dir.entries.find(entry_name);
    assert(it != dir.entries.end());

    auto entry_it = _inodes.find(it->second);
    assert(entry_it != _inodes.end());
    assert(entry_it->second.is_linked());

    --entry_it->second.directories_containing_file;
    dir.entries.erase(it);
}

void metadata_log::schedule_flush_of_curr_cluster() {
    // Make writes concurrent (TODO: maybe serialized within *one* cluster would be faster?)
    schedule_background_task(do_with(_curr_cluster_buff, &_device, [](auto& crr_clstr_bf, auto& device) {
        return crr_clstr_bf->flush_to_disk(*device);
    }));
}

future<> metadata_log::flush_curr_cluster() {
    if (_curr_cluster_buff->bytes_left_after_flush_if_done_now() == 0) {
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

metadata_log::flush_result metadata_log::schedule_flush_of_curr_cluster_and_change_it_to_new_one() {
    auto next_cluster = _cluster_allocator.alloc();
    if (not next_cluster) {
        // Here metadata log dies, we cannot even flush current cluster because from there we won't be able to recover
        // TODO: ^ add protection from it and take it into account during compaction
        return flush_result::NO_SPACE;
    }

    auto append_res = _curr_cluster_buff->append(ondisk_next_metadata_cluster {*next_cluster});
    assert(append_res == metadata_to_disk_buffer::APPENDED);
    schedule_flush_of_curr_cluster();

    // Make next cluster the current cluster to allow writing next metadata entries before flushing finishes
    _curr_cluster_buff->virtual_constructor();
    _curr_cluster_buff->init(_cluster_size, _alignment,
            cluster_id_to_offset(*next_cluster, _cluster_size));
    return flush_result::DONE;
}

void metadata_log::schedule_attempt_to_delete_inode(inode_t inode) {
    return schedule_background_task([this, inode] {
        auto it = _inodes.find(inode);
        if (it == _inodes.end() or it->second.is_linked() or it->second.is_open()) {
            return now(); // Scheduled delete became invalid
        }

        switch (append_ondisk_entry(ondisk_delete_inode {inode})) {
        case append_result::TOO_BIG:
            assert(false and "ondisk entry cannot be too big");
        case append_result::NO_SPACE:
            return make_exception_future(no_more_space_exception());
        case append_result::APPENDED:
            memory_only_delete_inode(inode);
            return now();
        }
        __builtin_unreachable();
    });
}

std::variant<inode_t, metadata_log::path_lookup_error> metadata_log::do_path_lookup(const std::string& path) const noexcept {
    if (path.empty() or path[0] != '/') {
        return path_lookup_error::NOT_ABSOLUTE;
    }

    std::vector<inode_t> components_stack = {_root_dir};
    size_t beg = 0;
    while (beg < path.size()) {
        range component_range = {beg, path.find('/', beg)};
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
            assert(component_range.beg > 0 and path[component_range.beg - 1] == '/' and "Since path is absolute we do not have to check if the current component is a directory");
            continue;
        } else if (component == "..") {
            if (components_stack.size() > 1) { // Root dir cannot be popped
                components_stack.pop_back();
            }
        } else {
            auto dir_it = _inodes.find(components_stack.back());
            assert(dir_it != _inodes.end() and "inode comes from some previous lookup (or is a root directory) hence dir_it has to be valid");
            assert(dir_it->second.is_directory() and "every previous component is a directory and it was checked when they were processed");
            auto& curr_dir = dir_it->second.get_directory();

            auto it = curr_dir.entries.find(component);
            if (it == curr_dir.entries.end()) {
                return path_lookup_error::NO_ENTRY;
            }

            inode_t entry_inode = it->second;
            if (check_if_dir) {
                auto entry_it = _inodes.find(entry_inode);
                assert(entry_it != _inodes.end() and "dir entries have to exist");
                if (not entry_it->second.is_directory()) {
                    return path_lookup_error::NOT_DIR;
                }
            }

            components_stack.emplace_back(entry_inode);
        }
    }

    return components_stack.back();
}

future<inode_t> metadata_log::path_lookup(const std::string& path) const {
    auto lookup_res = do_path_lookup(path);
    return std::visit(overloaded {
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
    }, lookup_res);
}

file_offset_t metadata_log::file_size(inode_t inode) const {
    auto it = _inodes.find(inode);
    if (it == _inodes.end()) {
        throw invalid_inode_exception();
    }

    return std::visit(overloaded {
        [](const inode_info::file& file) {
            return file.size();
        },
        [](const inode_info::directory&) -> file_offset_t {
            throw invalid_inode_exception();
        }
    }, it->second.contents);
}

future<> metadata_log::create_file(std::string path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), create_semantics::CREATE_FILE).discard_result();
}

future<inode_t> metadata_log::create_and_open_file(std::string path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), create_semantics::CREATE_AND_OPEN_FILE);
}

future<inode_t> metadata_log::create_and_open_unlinked_file(file_permissions perms) {
    return create_and_open_unlinked_file_operation::perform(*this, std::move(perms));
}

future<> metadata_log::create_directory(std::string path, file_permissions perms) {
    return create_file_operation::perform(*this, std::move(path), std::move(perms), create_semantics::CREATE_DIR).discard_result();
}

future<> metadata_log::link_file(inode_t inode, std::string path) {
    return link_file_operation::perform(*this, inode, std::move(path));
}

future<> metadata_log::link_file(std::string source, std::string destination) {
    return path_lookup(std::move(source)).then([this, destination = std::move(destination)](inode_t inode) {
        return link_file(inode, std::move(destination));
    });
}

future<> metadata_log::unlink_file(std::string path) {
    return unlink_or_remove_file_operation::perform(*this, std::move(path), remove_semantics::FILE_ONLY);
}

future<> metadata_log::remove_directory(std::string path) {
    return unlink_or_remove_file_operation::perform(*this, std::move(path), remove_semantics::FILE_OR_DIR);
}

future<> metadata_log::remove(std::string path) {
    return unlink_or_remove_file_operation::perform(*this, std::move(path), remove_semantics::DIR_ONLY);
}

future<inode_t> metadata_log::open_file(std::string path) {
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
        return _locks.with_lock(metadata_log::locks::shared {inode}, [this, inode_info = std::move(inode_info), inode] {
            if (not inode_exists(inode)) {
                return make_exception_future<inode_t>(operation_became_invalid_exception());
            }
            ++inode_info->opened_files_count;
            return make_ready_future<inode_t>(inode);
        });
    });
}

future<> metadata_log::close_file(inode_t inode) {
    auto inode_it = _inodes.find(inode);
    if (inode_it == _inodes.end()) {
        return make_exception_future(invalid_inode_exception());
    }
    inode_info* inode_info = &inode_it->second;
    if (inode_info->is_directory()) {
        return make_exception_future(is_directory_exception());
    }


    return _locks.with_lock(metadata_log::locks::shared {inode}, [this, inode, inode_info] {
        if (not inode_exists(inode)) {
            return make_exception_future(operation_became_invalid_exception());
        }

        assert(inode_info->is_open());

        --inode_info->opened_files_count;
        if (not inode_info->is_linked() and not inode_info->is_open()) {
            // Unlinked and not open file should be removed
            schedule_attempt_to_delete_inode(inode);
        }
        return now();
    });
}

future<size_t> metadata_log::write(inode_t inode, file_offset_t pos, const void* buffer, size_t len,
        const io_priority_class& pc) {
    return write_operation::perform(*this, inode, pos, buffer, len, pc);
}

future<> metadata_log::truncate(inode_t inode, file_offset_t size) {
    return truncate_operation::perform(*this, inode, size);
}

// TODO: think about how to make filesystem recoverable from ENOSPACE situation: flush() (or something else) throws ENOSPACE,
// then it should be possible to compact some data (e.g. by truncating a file) via top-level interface and retrying the flush()
// without a ENOSPACE error. In particular if we delete all files after ENOSPACE it should be successful. It becomes especially
// hard if we write metadata to the last cluster and there is no enough room to write these delete operations. We have to
// guarantee that the filesystem is in a recoverable state then.

} // namespace seastar::fs
