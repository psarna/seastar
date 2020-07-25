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

#include "fs/backend/inode_info.hh"
#include "fs/backend/metadata_log/entries.hh"
#include "fs/backend/shard.hh"
#include "fs/cluster_utils.hh"
#include "fs/unix_metadata.hh"

namespace mle = seastar::fs::backend::metadata_log::entries;

namespace seastar::fs::backend {

shard::shard(block_device device, disk_offset_t cluster_size, disk_offset_t alignment,
    shared_ptr<metadata_log::to_disk_buffer> metadata_log_cbuf,
    shared_ptr<Clock> clock)
: _device(std::move(device))
, _cluster_size(cluster_size)
, _alignment(alignment)
, _metadata_log_cbuf(std::move(metadata_log_cbuf))
, _cluster_allocator({}, {})
, _inode_allocator(1, 0)
, _clock(std::move(clock)) {
    assert(is_power_of_2(alignment));
    assert(cluster_size > 0 && cluster_size % alignment == 0);
}

shard::shard(block_device device, disk_offset_t cluster_size, disk_offset_t alignment)
: shard(std::move(device), cluster_size, alignment,
        make_shared<metadata_log::to_disk_buffer>(), make_shared<Clock>()) {}

future<> shard::shutdown() {
    return flush_log().then([this] {
        return _device.close();
    });
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

// TODO: think about how to make filesystem recoverable from ENOSPACE situation: flush() (or something else) throws ENOSPACE,
// then it should be possible to compact some data (e.g. by truncating a file) via top-level interface and retrying the flush()
// without a ENOSPACE error. In particular if we delete all files after ENOSPACE it should be successful. It becomes especially
// hard if we write metadata to the last cluster and there is no enough room to write these delete operations. We have to
// guarantee that the filesystem is in a recoverable state then.

} // namespace seastar::fs::backend
