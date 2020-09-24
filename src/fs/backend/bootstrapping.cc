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
#include "fs/backend/inode_info.hh"
#include "fs/backend/metadata_log/entries.hh"
#include "fs/backend/shard.hh"
#include "fs/cluster_utils.hh"
#include "fs/unix_metadata.hh"
#include "seastar/core/circular_buffer.hh"
#include "seastar/fs/overloaded.hh"
#include "seastar/util/defer.hh"
#include "seastar/util/log.hh"

#include <string_view>

using std::string_view;

namespace mle = seastar::fs::backend::metadata_log::entries;

namespace seastar::fs::backend {

namespace {
logger mlogger("fs_backend_shard_bootstrap");
} // namespace

future<> bootstrapping::bootstrap(shard& shard, inode_t root_dir, cluster_id_t first_metadata_cluster_id,
        cluster_range available_clusters, fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id) {
    // Reset shard to empty state
    shard._inodes.clear();
    shard._background_futures = now();
    shard._root_dir = root_dir;
    shard._inodes.emplace(root_dir, inode_info{
        .opened_files_count = 0,
        .links_count = 0,
        .metadata = {
            .ftype = file_type::DIRECTORY,
             // TODO: add other meaningful fields
        },
        .contents = inode_info::directory{}
    });

    return do_with(bootstrapping(shard, available_clusters),
            [first_metadata_cluster_id, fs_shards_pool_size, fs_shard_id](bootstrapping& bootstrap) {
                return bootstrap.bootstrap(first_metadata_cluster_id, fs_shards_pool_size, fs_shard_id);
            });
}

bootstrapping::bootstrapping(shard& shard, cluster_range available_clusters)
: _shard(shard)
, _available_clusters(available_clusters)
, _curr_cluster{ .content = decltype(_curr_cluster.content)::aligned(shard._alignment, shard._cluster_size) }
{}

future<> bootstrapping::bootstrap(cluster_id_t first_metadata_cluster_id, fs_shard_id_t fs_shards_pool_size,
        fs_shard_id_t fs_shard_id) {
    _next_cluster = first_metadata_cluster_id;
    mlogger.debug(">>>>  Started bootstrapping  <<<<");
    // TODO: disable all compaction during bootstrapping -- it cannot happen now because we just replay the metadata log
    return do_until([this] { return !_next_cluster; }, [this] {
        _curr_cluster.id = *_next_cluster;
        _next_cluster = std::nullopt;

        auto [it, inserted] = _metadata_log_clusters.emplace(_curr_cluster.id);
        if (!inserted) {
            return make_exception_future(ml_cluster_loop_exception());
        }
        return read_curr_cluster().then([this] {
            return bootstrap_curr_cluster();
        });
    }).then([this, fs_shards_pool_size, fs_shard_id] {
        mlogger.debug("Data bootstrapping is done");
        initialize_shard_metadata_log_cbuf();
        switch (initialize_shard_cluster_allocator()) {
        case SUCCESS: break;
        case METADATA_AND_DATA_LOG_OVERLAP: return make_exception_future(ml_and_dl_overlap());
        case NOSPACE: return make_exception_future(no_more_space_exception());
        }
        initialize_shard_inode_allocator(fs_shards_pool_size, fs_shard_id);
        // TODO: what about orphaned inodes: maybe they are remnants of unlinked files and we need to delete them,
        //       or maybe not?
        return now();
    });
}

void bootstrapping::initialize_shard_metadata_log_cbuf() {
    size_t pos = _shard._cluster_size - _curr_cluster.unparsed_content.size();
    mlogger.debug("Initializing _metadata_log_cbuf: cluster {}, pos {}", _curr_cluster.id, pos);
    _shard._metadata_log_cbuf = _shard._metadata_log_cbuf->virtual_constructor();
    _shard._metadata_log_cbuf->init_from_bootstrapped_cluster(_shard._cluster_size,
            _shard._alignment, cluster_id_to_offset(_curr_cluster.id, _shard._cluster_size), pos);
}

bootstrapping::ca_init_res bootstrapping::initialize_shard_cluster_allocator() {
    mlogger.debug("Initializing cluster allocator");
    std::unordered_set<cluster_id_t> taken_clusters;
    // TODO: make this asynchronous
    for (auto&& [inode, inode_info] : _shard._inodes) {
        if (!inode_info.is_file()) {
            continue;
        }
        for (auto&& [foffset, data_vec] : inode_info.get_file().data) {
            std::visit(overloaded{
                [](const inode_data_vec::in_mem_data&) {},
                [this, &taken_clusters](const inode_data_vec::on_disk_data& odd) {
                    auto cid = offset_to_cluster_id(odd.device_offset, _shard._cluster_size);
                    taken_clusters.emplace(cid);
                },
                [](const inode_data_vec::hole_data&) {},
            }, data_vec.data_location);
        }
    }
    // TODO: make this asynchronous
    // Check that data clusters don't overlap with metadata_log
    for (auto it = _metadata_log_clusters.begin(); it != _metadata_log_clusters.end();) {
        auto nh = _metadata_log_clusters.extract(it++);
        auto irt = taken_clusters.insert(std::move(nh));
        if (!irt.inserted) {
            mlogger.debug("^ Error: metadata and data log overlap by using the same cluster: {}", irt.node.value());
            return METADATA_AND_DATA_LOG_OVERLAP;
        }
    }

    // TODO: maybe check that writes do not overlap with other (still valid) writes on disk and do not use metadata_log clusters

    circular_buffer<cluster_id_t> free_clusters;
    for (auto cid : boost::irange(_available_clusters.beg, _available_clusters.end)) {
        if (taken_clusters.find(cid) == taken_clusters.end()) {
            free_clusters.emplace_back(cid);
        }
    }

    mlogger.debug("free clusters: {}", free_clusters.size());
    _shard._cluster_allocator.reset(std::move(taken_clusters), std::move(free_clusters));

    auto cid_opt = _shard._cluster_allocator.alloc();
    if (!cid_opt) {
        mlogger.debug("^ Error: could not allocate cluster for medium data log");
        return NOSPACE;
    }

    _shard._medium_data_log_cw = _shard._medium_data_log_cw->virtual_constructor();
    _shard._medium_data_log_cw->init(_shard._cluster_size, _shard._alignment,
            cluster_id_to_offset(*cid_opt, _shard._cluster_size));
    return SUCCESS;
}

void bootstrapping::initialize_shard_inode_allocator(fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id) {
    // Reset _inode_allocator
    std::optional<inode_t> latest_allocated_inode;
    if (!_shard._inodes.empty()) {
        latest_allocated_inode =_shard._inodes.rbegin()->first;
    }
    _shard._inode_allocator = shard_inode_allocator(fs_shards_pool_size, fs_shard_id, latest_allocated_inode);
}

future<> bootstrapping::read_curr_cluster() {
    disk_offset_t curr_cluster_disk_offset = cluster_id_to_offset(_curr_cluster.id, _shard._cluster_size);
    mlogger.debug("Bootstrapping from cluster {}...", _curr_cluster.id);
    return _shard._device.read(curr_cluster_disk_offset, _curr_cluster.content.get_write(),
            _shard._cluster_size).then([this](size_t bytes_read) {
        if (bytes_read != _shard._cluster_size) {
            mlogger.debug("^ Error: partial read {} of {}", bytes_read, _shard._cluster_size);
            return make_exception_future(failed_ml_cluster_read_exception());
        }

        mlogger.debug("Read cluster {}", _curr_cluster.id);
        _curr_cluster.unparsed_content =
            string_view(reinterpret_cast<const char*>(_curr_cluster.content.get()), _shard._cluster_size);
        return now();
    });
}

namespace {
enum [[nodiscard]] rv_checkpoint_error {
    NO_MEM,
    BUFF_TOO_SMALL,
    INVALID_ENTRY_TYPE,
    INVALID_CHECKSUM
};
} // namespace

// Returns checkpointed data or error
static val_or_err<string_view, rv_checkpoint_error> read_and_validate_checkpoint(string_view& buff, disk_offset_t cluster_size) {
    mlogger.debug("Processing checkpoint at {}", cluster_size - buff.size());
    auto rdres = mle::read<mle::checkpoint>(buff);
    if (!rdres) {
        switch (rdres.error()) {
        case mle::NO_MEM:
            mlogger.debug("^ Not enough memory");
            return NO_MEM;
        case mle::TOO_SMALL:
            mlogger.debug("^ buff it too small for checkpoint");
            return BUFF_TOO_SMALL;
        case mle::INVALID_ENTRY_TYPE:
            mlogger.debug("^ Entry type is invalid");
            return INVALID_ENTRY_TYPE;
        }
        __builtin_unreachable();
    }

    auto cp = rdres.value();
    if (cp.checkpointed_data_length > buff.size()) {
        mlogger.debug("^ checkpoint's data length is too big: {} > {}", cp.checkpointed_data_length, buff.size());
        return BUFF_TOO_SMALL;
    }

    boost::crc_32_type crc;
    crc.process_bytes(buff.data(), cp.checkpointed_data_length);
    crc.process_bytes(&cp.checkpointed_data_length, sizeof(cp.checkpointed_data_length));
    if (crc.checksum() != cp.crc32_checksum) {
        mlogger.debug("^ CRC code does not match: computed = {}, read = {}", crc.checksum(), (uint32_t)cp.crc32_checksum);
        return INVALID_CHECKSUM;
    }

    auto res = buff.substr(0, cp.checkpointed_data_length);
    buff.remove_prefix(cp.checkpointed_data_length);
    return res;
}

future<> bootstrapping::bootstrap_curr_cluster() {
    // Process cluster: the data layout format is:
    // | checkpoint1 | data1... | checkpoint2 | data2... | ... |
    return repeat([this]() -> future<stop_iteration> {
        assert(!_next_cluster); // Checkpointed mle::next_metadata_cluster ends bootstrapping of the current cluster
        // Align before reading checkpoint
        _curr_cluster.unparsed_content.remove_prefix(
            mod_by_power_of_2(_curr_cluster.unparsed_content.size(), _shard._alignment));
        deferred_action restore_unparsed_content = [this, bkp = _curr_cluster.unparsed_content] {
            _curr_cluster.unparsed_content = bkp;
        };

        auto rv_cp_res = read_and_validate_checkpoint(_curr_cluster.unparsed_content, _shard._cluster_size);
        if (!rv_cp_res) {
            switch (rv_cp_res.error()) {
            case NO_MEM: return make_exception_future<stop_iteration>(std::bad_alloc());
            case BUFF_TOO_SMALL:
            case INVALID_ENTRY_TYPE:
            case INVALID_CHECKSUM:
                mlogger.debug("^ Invalid checkpoint => bootstrapping ends");
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
        }

        _curr_cluster.curr_checkpoint = rv_cp_res.value();
        mlogger.debug("^ checkpoint is correct, size {}", _curr_cluster.curr_checkpoint.size());
        return bootstrap_checkpointed_data().then([this, ruc = std::move(restore_unparsed_content)] () mutable {
            ruc.cancel();
            return make_ready_future<stop_iteration>(stop_iteration(_next_cluster.has_value()));
        });
    }).then([] {
        mlogger.debug("Cluster bootstrapping finished");
    });
}

static auto invalid_entry_exception() {
    return make_exception_future(ml_invalid_entry_exception());
}

template<>
future<> bootstrapping::bootstrap_entry<mle::next_metadata_cluster>(mle::next_metadata_cluster& entry) {
    mlogger.debug("Entry: next metadata cluster is {}", entry.cluster_id);
    if (_next_cluster) {
        mlogger.debug("^ Error: only one mle::next_metadata_cluster may appear per cluster");
        return invalid_entry_exception();
    }
    if (_metadata_log_clusters.find(entry.cluster_id) != _metadata_log_clusters.end()) {
        mlogger.debug("^ Error: cluster is already used by metadata log (loops are forbidden)");
        return invalid_entry_exception();
    }
    _next_cluster = entry.cluster_id;
    return now();
}

template<>
future<> bootstrapping::bootstrap_entry<mle::create_inode>(mle::create_inode& entry) {
    mlogger.debug("Entry: create inode {} of type {}", entry.inode, [&entry] {
        switch (entry.metadata.ftype) {
        case file_type::REGULAR_FILE: return "REG";
        case file_type::DIRECTORY: return "DIR";
        }
        return "???";
    }());
    if (inode_exists(entry.inode)) {
        mlogger.debug("^ Error: inode already exist");
        return invalid_entry_exception();
    }

    _shard.memory_only_create_inode(entry.inode, entry.metadata);
    return now();
}

bool bootstrapping::delete_inode_is_valid(const mle::delete_inode& entry) const noexcept {
    if (!inode_exists(entry.inode)) {
        mlogger.debug("^ Error: inode does not exist");
        return false;
    }
    const inode_info& inode_info = _shard._inodes.at(entry.inode);
    if (inode_info.is_directory() && !inode_info.get_directory().entries.empty()) {
        mlogger.debug("^ Error: cannot delete inode that is a non-empty directory");
        return false;
    }
    if (inode_info.links_count > 0) {
        mlogger.debug("^ Error: only unlinked inodes may be deleted");
        return false;
    }

    return true;
}

template<>
future<> bootstrapping::bootstrap_entry<mle::delete_inode>(mle::delete_inode& entry) {
    mlogger.debug("Entry: delete inode {}", entry.inode);
    if (!delete_inode_is_valid(entry)) {
        return invalid_entry_exception();
    }
    _shard.memory_only_delete_inode(entry.inode);
    return now();
}

template<>
future<> bootstrapping::bootstrap_entry<mle::small_write>(mle::small_write& entry) {
    mlogger.debug("Entry: small write to {} at {} of size {}", entry.inode, entry.offset, entry.data.size());
    if (!inode_exists(entry.inode)) {
        mlogger.debug("^ Error: inode does not exist");
        return invalid_entry_exception();
    }
    if (!_shard._inodes[entry.inode].is_file()) {
        mlogger.debug("^ Error: inode is not a regular file");
        return invalid_entry_exception();
    }

    _shard.memory_only_small_write(entry.inode, entry.offset, std::move(entry.data));
    _shard.memory_only_update_mtime(entry.inode, entry.time_ns);
    return now();
}

template<>
future<> bootstrapping::bootstrap_entry<mle::medium_write>(mle::medium_write& entry) {
    mlogger.debug("Entry: medium write to {} at {}, placed on disk at [{}, {})", entry.inode, entry.offset, entry.drange.beg, entry.drange.end);
    if (!inode_exists(entry.inode)) {
        mlogger.debug("^ Error: inode does not exist");
        return invalid_entry_exception();
    }
    if (!_shard._inodes[entry.inode].is_file()) {
        mlogger.debug("^ Error: inode is not a regular file");
        return invalid_entry_exception();
    }

    cluster_id_t beg_cluster_id = offset_to_cluster_id(entry.drange.beg, _shard._cluster_size);
    cluster_id_t end_cluster_id = offset_to_cluster_id(entry.drange.end, _shard._cluster_size);
    if (entry.drange.beg >= entry.drange.end || beg_cluster_id != end_cluster_id) {
        mlogger.debug("^ Error: disk offset range is invalid");
        return invalid_entry_exception();
    }
    if (beg_cluster_id < _available_clusters.beg || _available_clusters.end <= beg_cluster_id) {
        mlogger.debug("^ Error: cluster id is out of range");
        return invalid_entry_exception();
    }

    _shard.memory_only_disk_write(entry.inode, entry.offset, entry.drange.beg, entry.drange.size());
    _shard.memory_only_update_mtime(entry.inode, entry.time_ns);
    return now();
}

template<>
future<> bootstrapping::bootstrap_entry<mle::large_write_without_time>(mle::large_write_without_time& entry) {
    mlogger.debug("Entry: large write to {} at {}, placed in cluster: {}", entry.inode, entry.offset, entry.data_cluster);
    if (!inode_exists(entry.inode)) {
        mlogger.debug("^ Error: inode does not exist");
        return invalid_entry_exception();
    }
    if (!_shard._inodes[entry.inode].is_file()) {
        mlogger.debug("^ Error: inode is not a regular file");
        return invalid_entry_exception();
    }
    if (entry.data_cluster < _available_clusters.beg || _available_clusters.end <= entry.data_cluster) {
        mlogger.debug("^ Error: cluster id is out of range");
        return invalid_entry_exception();
    }

    _shard.memory_only_disk_write(entry.inode, entry.offset,
            cluster_id_to_offset(entry.data_cluster, _shard._cluster_size), _shard._cluster_size);
    return now();
}

template<>
future<> bootstrapping::bootstrap_entry<mle::large_write>(mle::large_write& entry) {
    return bootstrap_entry(entry.lwwt).then([this, entry] {
        _shard.memory_only_update_mtime(entry.lwwt.inode, entry.time_ns);
    });
}

template<>
future<> bootstrapping::bootstrap_entry<mle::truncate>(mle::truncate& entry) {
    mlogger.debug("Entry: truncate {} to size {}", entry.inode, entry.size);
    if (!inode_exists(entry.inode)) {
        mlogger.debug("^ Error: inode does not exist");
        return invalid_entry_exception();
    }
    if (!_shard._inodes[entry.inode].is_file()) {
        mlogger.debug("^ Error: inode is not a regular file");
        return invalid_entry_exception();
    }

    _shard.memory_only_truncate(entry.inode, entry.size);
    _shard.memory_only_update_mtime(entry.inode, entry.time_ns);
    return now();
}

struct create_dentry_with_flags {
    inode_t inode;
    std::string& name;
    inode_t dir_inode;
    bool allow_linking_directory;
};

template<>
future<> bootstrapping::bootstrap_entry<create_dentry_with_flags>(create_dentry_with_flags& entry) {
    mlogger.debug("Entry: create dentry in {}: \"{}\" -> {}", entry.dir_inode, entry.name, entry.inode);
    if (!inode_exists(entry.inode)) {
        mlogger.debug("^ Error: dentry inode does not exist");
        return invalid_entry_exception();
    }
    if (!inode_exists(entry.dir_inode)) {
        mlogger.debug("^ Error: dir inode does not exist");
        return invalid_entry_exception();
    }

    auto& inode_info = _shard._inodes.at(entry.inode);
    auto& dir_inode_info = _shard._inodes.at(entry.dir_inode);
    if (!entry.allow_linking_directory && inode_info.is_directory()) {
        mlogger.debug("^ Error: cannot link directory");
        return invalid_entry_exception();
    }
    if (!dir_inode_info.is_directory()) {
        mlogger.debug("^ Error: dir inode is not a directory");
        return invalid_entry_exception();
    }
    auto& dir = dir_inode_info.get_directory();
    if (dir.entries.count(entry.name) != 0) {
        mlogger.debug("^ Error: name is taken by other dentry");
        return invalid_entry_exception();
    }

    _shard.memory_only_create_dentry(dir, entry.inode, std::move(entry.name));
    // TODO: Maybe time_ns for modifying directory?
    return now();
}

template<>
future<> bootstrapping::bootstrap_entry<mle::create_dentry>(mle::create_dentry& entry) {
    create_dentry_with_flags new_entry = {
        .inode = entry.inode,
        .name = entry.name,
        .dir_inode = entry.dir_inode,
        .allow_linking_directory = false, // Only files may be linked as not to create cycles (directories are
                                          // created and linked using mle::create_inode_as_dentry
    };
    // Taking reference here is OK, because new_entry won't be used as soon as future<> will be constructed
    return bootstrap_entry(new_entry);
}

template<>
future<> bootstrapping::bootstrap_entry<mle::create_inode_as_dentry>(mle::create_inode_as_dentry& entry) {
    mlogger.debug("Compound entry: create inode as dentry");
    return bootstrap_entry(entry.inode).then([this, &entry] {
        deferred_action rollback = [this, inode = entry.inode.inode] {
            _shard.memory_only_delete_inode(inode);
        };

        create_dentry_with_flags new_entry = {
            .inode = entry.inode.inode,
            .name = entry.name,
            .dir_inode = entry.dir_inode,
            .allow_linking_directory = true,
        };

        // Taking reference here is OK, because new_entry won't be used as soon as future<> will be constructed
        return bootstrap_entry(new_entry).then([rollback = std::move(rollback)]() mutable {
            rollback.cancel();
        });
    });

    return now();
}

template<>
future<> bootstrapping::bootstrap_entry<mle::delete_dentry>(mle::delete_dentry& entry) {
    mlogger.debug("Entry: delete dentry in {}: \"{}\"", entry.dir_inode, entry.name);
    if (!inode_exists(entry.dir_inode)) {
        mlogger.debug("^ Error: dir inode does not exist");
        return invalid_entry_exception();
    }

    auto& dir_inode_info = _shard._inodes.at(entry.dir_inode);
    if (!dir_inode_info.is_directory()) {
        mlogger.debug("^ Error: dir inode is not a directory");
        return invalid_entry_exception();
    }
    auto& dir = dir_inode_info.get_directory();
    if (dir.entries.find(entry.name) == dir.entries.end()) {
        mlogger.debug("^ Error: dentry does not exist");
        return invalid_entry_exception();
    }

    _shard.memory_only_delete_dentry(dir, entry.name);
    // TODO: Maybe time_ns for modifying directory?
    return now();
}

template<>
future<> bootstrapping::bootstrap_entry<mle::delete_inode_and_dentry>(mle::delete_inode_and_dentry& entry) {
    mlogger.debug("Compound entry: delete inode {} and delete dentry", entry.di.inode);
    // We have 2 cases:
    // - deleting dir entry and inode that it points to
    // - deleting dir entry and inode that are unrelated
    // Third case: deleting dir entry and inode of this dir is not possible as we do not permit unlinked directories
    if (!delete_inode_is_valid(entry.di)) {
        return invalid_entry_exception();
    }

    return bootstrap_entry(entry.dd).then([this, &entry] {
        assert(delete_inode_is_valid(entry.di));
        _shard.memory_only_delete_inode(entry.di.inode);
    });
}

bool bootstrapping::inode_exists(inode_t inode) const noexcept {
    return _shard._inodes.find(inode) != _shard._inodes.end();
}

future<> bootstrapping::bootstrap_checkpointed_data() {
    return do_until([this] { return _curr_cluster.curr_checkpoint.empty(); }, [this] {
        auto rd_res = metadata_log::entries::read_any(_curr_cluster.curr_checkpoint);
        if (!rd_res) {
            switch (rd_res.error()) {
            case mle::NO_MEM:
                mlogger.debug("Error: reading entry: not enough memory");
                return make_exception_future(std::bad_alloc());
            case mle::TOO_SMALL:
                mlogger.debug("Error: reading entry: entry does not fit, although checkpoint tells otherwise");
                return invalid_entry_exception();
            case mle::INVALID_ENTRY_TYPE:
                mlogger.debug("Error: reading entry: invalid entry type, although checkpoint tells it should be valid");
                return invalid_entry_exception();
            }
        }

        return do_with(std::move(rd_res.value()), [this](auto& val) {
            return std::visit(overloaded{
                [](mle::checkpoint&) {
                    mlogger.debug("Error: checkpoint cannot appear as part of checkpointed data");
                    return invalid_entry_exception();
                },
                [this](auto& entry) { return this->bootstrap_entry(entry); },
            }, val);
        });
    });
}

} // namespace seastar::fs::backend
