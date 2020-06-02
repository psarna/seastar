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
#include "fs/data_cluster_contents_info.hh"
#include "fs/data_compaction.hh"
#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "fs/metadata_log_bootstrap.hh"
#include "fs/metadata_log_operations/create_and_open_unlinked_file.hh"
#include "fs/metadata_log_operations/create_file.hh"
#include "fs/metadata_log_operations/link_file.hh"
#include "fs/metadata_log_operations/read.hh"
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
#include "seastar/fs/exceptions.hh"
#include "seastar/fs/overloaded.hh"
#include "seastar/fs/stat.hh"

#include <boost/crc.hpp>
#include <boost/range/irange.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <unordered_set>
#include <variant>

namespace seastar::fs {

namespace {
logger mlogger("fs_metadata_log");
} // namespace

metadata_log::metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment,
    shared_ptr<metadata_to_disk_buffer> cluster_buff, shared_ptr<cluster_writer> data_writer,
    double min_compactness, size_t max_data_compaction_memory)
: _device(std::move(device))
, _cluster_size(cluster_size)
, _alignment(alignment)
, _curr_cluster_buff(std::move(cluster_buff))
, _curr_data_writer(std::move(data_writer))
, _inode_allocator(1, 0)
, _min_compactness(min_compactness)
, _max_data_compaction_memory(max_data_compaction_memory) {
    assert(is_power_of_2(alignment));
    assert(cluster_size > 0 and cluster_size % alignment == 0);
    assert(min_compactness > 0 || ((cluster_id_t) (max_data_compaction_memory / (min_compactness * cluster_size))) * (1-min_compactness) >= 1);
}

metadata_log::metadata_log(block_device device, unit_size_t cluster_size, unit_size_t alignment,
    double min_compactness, size_t max_data_compaction_memory)
: metadata_log(std::move(device), cluster_size, alignment,
        make_shared<metadata_to_disk_buffer>(), make_shared<cluster_writer>(),
        min_compactness, max_data_compaction_memory) {}

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

void metadata_log::throw_if_read_only_fs() {
    if (__builtin_expect(_read_only_fs == read_only_fs::yes, false)) {
        throw read_only_filesystem_exception();
    }
}

void metadata_log::set_fs_read_only_mode(read_only_fs val) noexcept {
    mlogger.warn("Turned {} read-only mode.", val == read_only_fs::yes ? "on" : "off");
    _read_only_fs = val;
}

void metadata_log::write_update(inode_info::file& file, inode_data_vec new_data_vec) {
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
    file.cut_out_data_range(range, [&](const inode_data_vec& former, const inode_data_vec& left_remains, const inode_data_vec& right_remains) {
        // Remove former before adding remains
        std::visit(overloaded {
            [&](const inode_data_vec::in_mem_data&) {
                _compacted_log_size -= ondisk_entry_size<ondisk_small_write_header>(former.data_range.size());
            },
            [&](const inode_data_vec::on_disk_data& disk_data) {
                // TODO: Differentiate between large write and large write without mtime
                _compacted_log_size -= (former.data_range.size() == _cluster_size ?
                    ondisk_entry_size<ondisk_large_write>() :
                    ondisk_entry_size<ondisk_medium_write>());
                cluster_id_t clst_id = offset_to_cluster_id(disk_data.device_offset, _cluster_size);
                auto it = _data_cluster_contents_info_map.find(clst_id);
                assert(it != _data_cluster_contents_info_map.end());
                size_t pre_cut_cluster_data_size = it->second->get_up_to_date_data_size();
                it->second->cut_data(disk_data.device_offset, former.data_range, left_remains.data_range, right_remains.data_range);
                size_t post_cut_cluster_data_size = it->second->get_up_to_date_data_size();
                update_cluster_data_size(it->first, pre_cut_cluster_data_size, post_cut_cluster_data_size);
            },
            [&](const inode_data_vec::hole_data&) {
            },
        }, former.data_location);

        auto visit_remains = [&](const inode_data_vec& data_vec) {
            if (data_vec.data_range.is_empty()) {
                return;
            }
            std::visit(overloaded {
                [&](const inode_data_vec::in_mem_data&) {
                    _compacted_log_size += ondisk_entry_size<ondisk_small_write_header>(data_vec.data_range.size());
                },
                [&](const inode_data_vec::on_disk_data& disk_data) {
                    // Remaining data is smaller than original so it is not a large write anymore
                    assert(data_vec.data_range.size() < _cluster_size);
                    _compacted_log_size += ondisk_entry_size<ondisk_medium_write>();
                },
                [&](const inode_data_vec::hole_data&) {
                },
            }, data_vec.data_location);
        };
        visit_remains(left_remains);
        visit_remains(right_remains);

        // TODO: empty clusters compaction: we could check here if updated cluster has no up-to-date data and is
        //       read-only. If so we could schedule compaction of that cluster in order to immediately move it to
        //       _cluster_allocator.
    });
}

inode_info& metadata_log::memory_only_create_inode(inode_t inode, bool is_directory, unix_metadata metadata) {
    assert(_inodes.count(inode) == 0);
    _compacted_log_size += ondisk_entry_size<ondisk_create_inode>();
    if (!is_directory) {
        // During rewrite we'll start with truncating the file to its final size to remember trailing holes
        _compacted_log_size += ondisk_entry_size<ondisk_truncate>();
    }
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
        [&](inode_info::file& file) {
            cut_out_data_range(file, {
                .beg = 0,
                .end = std::numeric_limits<decltype(file_range::end)>::max()
            });
            _compacted_log_size -= ondisk_entry_size<ondisk_truncate>(); // See memory_only_create_inode() for why it is here
        }
    }, it->second.contents);

    _inodes.erase(it);
    _compacted_log_size -= ondisk_entry_size<ondisk_create_inode>();
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
    _compacted_log_size += ondisk_entry_size<ondisk_small_write_header>(data.size());
}

void metadata_log::memory_only_disk_write(inode_t inode, file_offset_t file_offset, disk_offset_t disk_offset,
        size_t write_len) {
    assert(write_len <= _cluster_size);
    inode_data_vec data_vec = {
        {file_offset, file_offset + write_len},
        inode_data_vec::on_disk_data {disk_offset}
    };
    auto it = _inodes.find(inode);
    assert(it != _inodes.end());
    assert(it->second.is_file());
    write_update(it->second.get_file(), std::move(data_vec));
    _compacted_log_size += (write_len == _cluster_size ?
        ondisk_entry_size<ondisk_large_write>() :
        ondisk_entry_size<ondisk_medium_write>());
    cluster_id_t cluster_id = offset_to_cluster_id(disk_offset, _cluster_size);
    assert(_read_only_data_clusters.find(cluster_id) == _read_only_data_clusters.end());
    auto [cc_info_it, created] = _writable_data_clusters.try_emplace(cluster_id);
    auto& cc_info = cc_info_it->second;
    cc_info.add_data(disk_offset, inode, {file_offset, file_offset + write_len});
    if (created) {
        mlogger.debug("adding cluster to writable: {}", cluster_id);
        auto [_, inserted] = _data_cluster_contents_info_map.try_emplace(cluster_id, &cc_info);
        assert(inserted);
    }
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

    if (it->second.directories_containing_file == 1) {
        // We know that we'll write ondisk_create_inode_as_dir_entry_header (instead of ondisk_create_inode and ondisk_add_dir_entry)
        // in rewrite log because the entry is linked for the first time
        _compacted_log_size += ondisk_entry_size<ondisk_create_inode_as_dir_entry_header>(entry_name.size()) -
            ondisk_entry_size<ondisk_create_inode>();
    } else {
        _compacted_log_size += ondisk_entry_size<ondisk_add_dir_entry_header>(entry_name.size());
    }
}

void metadata_log::memory_only_delete_dir_entry(inode_info::directory& dir, std::string entry_name) {
    auto it = dir.entries.find(entry_name);
    assert(it != dir.entries.end());

    auto entry_it = _inodes.find(it->second);
    assert(entry_it != _inodes.end());
    assert(entry_it->second.is_linked());

    --entry_it->second.directories_containing_file;
    dir.entries.erase(it);

    if (__builtin_expect(entry_it->second.directories_containing_file == 0, false)) {
        // We were going to write ondisk_create_inode_as_dir_entry_header in rewrite log but we can't anymore
        _compacted_log_size -= ondisk_entry_size<ondisk_create_inode_as_dir_entry_header>(entry_name.size()) -
            ondisk_entry_size<ondisk_create_inode>();
    } else {
        _compacted_log_size -= ondisk_entry_size<ondisk_add_dir_entry_header>(entry_name.size());
    }
}

std::optional<cluster_id_t> metadata_log::alloc_cluster() noexcept {
    std::optional<cluster_id_t> ret = _cluster_allocator.alloc();
    if (ret) {
        update_compactness();
    }
    return ret;
}
future<std::vector<cluster_id_t>> metadata_log::alloc_clusters_wait(size_t count) {
    return _cluster_allocator.alloc_wait(count).then([this](std::vector<cluster_id_t> cluster_ids){
        update_compactness();
        return make_ready_future<std::vector<cluster_id_t>>(std::move(cluster_ids));
    });
}
void metadata_log::free_cluster(cluster_id_t cluster_id) noexcept {
    _cluster_allocator.free(cluster_id);
    update_compactness();
}
void metadata_log::free_clusters(const std::vector<cluster_id_t>& cluster_ids) noexcept {
    free_clusters(cluster_ids);
    update_compactness();
}

void metadata_log::finish_writing_data_cluster(cluster_id_t cluster_id) {
    mlogger.debug("moving cluster to read only: {}", cluster_id);
    auto nh = _writable_data_clusters.extract(cluster_id);
    assert(!nh.empty());
    auto insert_res = _read_only_data_clusters.insert(std::move(nh));
    assert(insert_res.inserted);
    size_t up_to_date_size = insert_res.position->second.get_up_to_date_data_size();
    try_compacting_data_cluster(cluster_id, up_to_date_size);
}

void metadata_log::make_data_cluster_writable(cluster_id_t cluster_id) {
    mlogger.debug("moving cluster to writable: {}", cluster_id);
    auto nh = _read_only_data_clusters.extract(cluster_id);
    assert(!nh.empty());
    auto insert_res = _writable_data_clusters.insert(std::move(nh));
    assert(insert_res.inserted);
}

void metadata_log::free_writable_data_cluster(cluster_id_t cluster_id) noexcept {
    mlogger.debug("freeing cluster from writable: {}", cluster_id);
    auto num = _data_cluster_contents_info_map.erase(cluster_id);
    assert(num == 1);
    num = _writable_data_clusters.erase(cluster_id);
    assert(num == 1);
    free_cluster(cluster_id);
}

void metadata_log::try_compacting_data_cluster(cluster_id_t cluster_id, size_t size) {
    if (size > _current_compactness * _cluster_size) {
        _compaction_awaiting_data_clusters.insert(std::make_pair(size, cluster_id));
    } else if (size == 0) {
        mlogger.debug("Running compaction on empty cluster {}", cluster_id);
        schedule_background_task([this, cluster_id] {return compact_data_clusters(std::vector<cluster_id_t>{cluster_id});});
    } else {
        mlogger.debug("Adding data cluster {} to compaction", cluster_id);
        if (_compaction_ready_data_size + size > _max_data_compaction_memory) {
            mlogger.debug("Running compaction on clusters {}", _compaction_ready_data_clusters);
            std::vector<cluster_id_t> move_vec = {};
            _compaction_ready_data_clusters.swap(move_vec);
            schedule_background_compaction([this, move_vec = std::move(move_vec)] {return compact_data_clusters(std::move(move_vec));});
        }
        _compaction_ready_data_clusters.push_back(cluster_id);
    }
}

void metadata_log::update_cluster_data_size(cluster_id_t cluster_id, size_t old_size, size_t new_size) {
    if (_read_only_data_clusters.count(cluster_id) != 1) {
        return;
    }
    if (_compaction_awaiting_data_clusters.erase(std::make_pair(old_size, cluster_id)) == 1) {
        // Otherwise it's already being compacted
        try_compacting_data_cluster(cluster_id, new_size);
    }
}

void metadata_log::update_compactness() {
    _current_compactness = _min_compactness * _cluster_allocator.allocated_size() / _cluster_allocator.size();
    while (!_compaction_awaiting_data_clusters.empty()) {
        auto it = _compaction_awaiting_data_clusters.begin();
        if (it->first > _current_compactness * _cluster_size) {
            break;
        }
        auto nh = _compaction_awaiting_data_clusters.extract(it);
        assert(!nh.empty());
        auto [cluster_data_size, cluster_id] = nh.value();
        try_compacting_data_cluster(cluster_id, cluster_data_size);
    }
}

void metadata_log::schedule_flush_of_curr_cluster() {
    throw_if_read_only_fs();
    // Make writes concurrent (TODO: maybe serialized within *one* cluster would be faster?)
    schedule_background_task(do_with(_curr_cluster_buff, &_device, [this](auto& crr_clstr_bf, auto& device) {
        throw_if_read_only_fs();
        return crr_clstr_bf->flush_to_disk(*device).handle_exception([this](std::exception_ptr ptr) {
            set_fs_read_only_mode(read_only_fs::yes);
            return make_exception_future(std::move(ptr));
        });
    }));
}

future<> metadata_log::flush_curr_cluster() {
    try {
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
    } catch(...) {
        return seastar::make_exception_future(std::current_exception());
    }

    return _background_futures.get_future();
}

metadata_log::flush_result metadata_log::schedule_flush_of_curr_cluster_and_change_it_to_new_one() {
    throw_if_read_only_fs();

    auto next_cluster = alloc_cluster();
    if (not next_cluster) {
        // Here metadata log dies, we cannot even flush current cluster because from there we won't be able to recover
        // TODO: ^ add protection from it
        return flush_result::NO_SPACE;
    }

    auto append_res = _curr_cluster_buff->append(ondisk_next_metadata_cluster {*next_cluster});
    assert(append_res == metadata_to_disk_buffer::APPENDED);
    ++_log_cluster_count;
    schedule_flush_of_curr_cluster();

    // Make next cluster the current cluster to allow writing next metadata entries before flushing finishes
    _curr_cluster_buff = _curr_cluster_buff->virtual_constructor();
    _curr_cluster_buff->init(_cluster_size, _alignment,
            cluster_id_to_offset(*next_cluster, _cluster_size));
    return flush_result::DONE;
}

void metadata_log::schedule_attempt_to_delete_inode(inode_t inode) {
    throw_if_read_only_fs();
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
    }, do_path_lookup(path));
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

stat_data metadata_log::stat(inode_t inode) const {
    auto it = _inodes.find(inode);
    if (it == _inodes.end())
        throw invalid_inode_exception();

    const inode_info& inode_info = it->second;
    return {
        std::visit(overloaded {
            [](const inode_info::file&) { return directory_entry_type::regular; },
            [](const inode_info::directory&) { return directory_entry_type::directory; },
        }, inode_info.contents),
        inode_info.metadata.perms,
        inode_info.metadata.uid,
        inode_info.metadata.gid,
        std::chrono::system_clock::time_point(std::chrono::nanoseconds(inode_info.metadata.btime_ns)),
        std::chrono::system_clock::time_point(std::chrono::nanoseconds(inode_info.metadata.mtime_ns)),
        std::chrono::system_clock::time_point(std::chrono::nanoseconds(inode_info.metadata.ctime_ns)),
    };
}

stat_data metadata_log::stat(const std::string& path) const {
    return std::visit(overloaded {
        [](path_lookup_error error) -> stat_data {
            switch (error) {
            case path_lookup_error::NOT_ABSOLUTE:
                throw path_is_not_absolute_exception();
            case path_lookup_error::NO_ENTRY:
                throw no_such_file_or_directory_exception();
            case path_lookup_error::NOT_DIR:
                throw path_component_not_directory_exception();
            }
            __builtin_unreachable();
        },
        [this](inode_t inode) {
            return stat(inode);
        }
    }, do_path_lookup(path));
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
    return unlink_or_remove_file_operation::perform(*this, std::move(path), remove_semantics::DIR_ONLY);
}

future<> metadata_log::remove(std::string path) {
    return unlink_or_remove_file_operation::perform(*this, std::move(path), remove_semantics::FILE_OR_DIR);
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

future<size_t> metadata_log::read(inode_t inode, file_offset_t pos, void* buffer, size_t len,
        const io_priority_class& pc) {
    return read_operation::perform(*this, inode, pos, buffer, len, pc);
}

future<size_t> metadata_log::write(inode_t inode, file_offset_t pos, const void* buffer, size_t len,
        const io_priority_class& pc) {
    return write_operation::perform(*this, inode, pos, buffer, len, pc);
}

future<> metadata_log::truncate(inode_t inode, file_offset_t size) {
    return truncate_operation::perform(*this, inode, size);
}

future<> metadata_log::compact_data_clusters(std::vector<cluster_id_t> cluster_ids) {
    return data_compaction::perform(*this, std::move(cluster_ids));
}

// TODO: think about how to make filesystem recoverable from ENOSPACE situation: flush() (or something else) throws ENOSPACE,
// then it should be possible to compact some data (e.g. by truncating a file) via top-level interface and retrying the flush()
// without a ENOSPACE error. In particular if we delete all files after ENOSPACE it should be successful. It becomes especially
// hard if we write metadata to the last cluster and there is no enough room to write these delete operations. We have to
// guarantee that the filesystem is in a recoverable state then.

} // namespace seastar::fs
