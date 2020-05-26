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

#include "fs/bitwise.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log_bootstrap.hh"
#include "seastar/util/log.hh"

namespace seastar::fs {

namespace {
logger mlogger("fs_metadata_bootstrap");
} // namespace

bool data_reader::read(void* destination, size_t size) {
    if (_pos + size > _size) {
        return false;
    }

    std::memcpy(destination, _data + _pos, size);
    _pos += size;
    return true;
}

bool data_reader::read_string(std::string& str, size_t size) {
    str.resize(size);
    return read(str.data(), size);
}

std::optional<temporary_buffer<uint8_t>> data_reader::read_tmp_buff(size_t size) {
    if (_pos + size > _size) {
        return std::nullopt;
    }

    _pos += size;
    return temporary_buffer<uint8_t>(_data + _pos - size, size);
}

bool data_reader::process_crc_without_reading(boost::crc_32_type& crc, size_t size) {
    if (_pos + size > _size) {
        return false;
    }

    crc.process_bytes(_data + _pos, size);
    return true;
}

std::optional<data_reader> data_reader::extract(size_t size) {
    if (_pos + size > _size) {
        return std::nullopt;
    }

    _pos += size;
    return data_reader(_data + _pos - size, size);
}

metadata_log_bootstrap::metadata_log_bootstrap(metadata_log& metadata_log, cluster_range available_clusters)
: _metadata_log(metadata_log)
, _available_clusters(available_clusters)
, _curr_cluster_data(decltype(_curr_cluster_data)::aligned(metadata_log._alignment, metadata_log._cluster_size))
{}

future<> metadata_log_bootstrap::bootstrap(cluster_id_t first_metadata_cluster_id, fs_shard_id_t fs_shards_pool_size,
        fs_shard_id_t fs_shard_id) {
    _next_cluster = first_metadata_cluster_id;
    mlogger.debug(">>>>  Started bootstraping  <<<<");
    return do_with((cluster_id_t)first_metadata_cluster_id, [this](cluster_id_t& last_cluster) {
        return do_until([this] { return not _next_cluster.has_value(); }, [this, &last_cluster] {
            cluster_id_t curr_cluster = *_next_cluster;
            _next_cluster = std::nullopt;
            bool inserted = _taken_clusters.emplace(curr_cluster).second;
            assert(inserted); // TODO: check it in next_cluster record
            last_cluster = curr_cluster;
            return bootstrap_cluster(curr_cluster);
        }).then([this, &last_cluster] {
            mlogger.debug("Data bootstraping is done");
            // Initialize _curr_cluster_buff
            _metadata_log._curr_cluster_buff = _metadata_log._curr_cluster_buff->virtual_constructor();
            mlogger.debug("Initializing _curr_cluster_buff: cluster {}, pos {}", last_cluster, _curr_cluster.last_checkpointed_pos());
            _metadata_log._curr_cluster_buff->init_from_bootstrapped_cluster(_metadata_log._cluster_size,
                    _metadata_log._alignment, cluster_id_to_offset(last_cluster, _metadata_log._cluster_size),
                    _curr_cluster.last_checkpointed_pos());
        });
    }).then([this, fs_shards_pool_size, fs_shard_id] {
        // Initialize _cluser_allocator
        mlogger.debug("Initializing cluster allocator");
        circular_buffer<cluster_id_t> free_clusters;
        for (auto cid : boost::irange(_available_clusters.beg, _available_clusters.end)) {
            if (_taken_clusters.count(cid) == 0) {
                free_clusters.emplace_back(cid);
            }
        }
        if (free_clusters.empty()) {
            return make_exception_future(no_more_space_exception());
        }
        cluster_id_t datalog_cluster_id = free_clusters.front();
        _taken_clusters.emplace(datalog_cluster_id);
        free_clusters.pop_front();

        _metadata_log._curr_data_writer = _metadata_log._curr_data_writer->virtual_constructor();
        _metadata_log._curr_data_writer->init(_metadata_log._cluster_size, _metadata_log._alignment,
                cluster_id_to_offset(datalog_cluster_id, _metadata_log._cluster_size));

        mlogger.debug("free clusters: {}", free_clusters.size());
        _metadata_log._cluster_allocator.reset(std::move(_taken_clusters), std::move(free_clusters));

        // Reset _inode_allocator
        std::optional<inode_t> max_inode_no;
        if (not _metadata_log._inodes.empty()) {
            max_inode_no =_metadata_log._inodes.rbegin()->first;
        }
        _metadata_log._inode_allocator = shard_inode_allocator(fs_shards_pool_size, fs_shard_id, max_inode_no);

        // TODO: what about orphaned inodes: maybe they are remnants of unlinked files and we need to delete them,
        //       or maybe not?
        return now();
    });
}

future<> metadata_log_bootstrap::bootstrap_cluster(cluster_id_t curr_cluster) {
    disk_offset_t curr_cluster_disk_offset = cluster_id_to_offset(curr_cluster, _metadata_log._cluster_size);
    mlogger.debug("Bootstraping from cluster {}...", curr_cluster);
    return _metadata_log._device.read(curr_cluster_disk_offset, _curr_cluster_data.get_write(),
            _metadata_log._cluster_size).then([this, curr_cluster](size_t bytes_read) {
        if (bytes_read != _metadata_log._cluster_size) {
            return make_exception_future(std::runtime_error("Failed to read whole cluster of the metadata log"));
        }

        mlogger.debug("Read cluster {}", curr_cluster);
        _curr_cluster = data_reader(_curr_cluster_data.get(), _metadata_log._cluster_size);
        return bootstrap_read_cluster();
    });
}

future<> metadata_log_bootstrap::bootstrap_read_cluster() {
    // Process cluster: the data layout format is:
    // | checkpoint1 | data1... | checkpoint2 | data2... | ... |
    return do_with(false, [this](bool& whole_log_ended) {
        return do_until([this, &whole_log_ended] { return whole_log_ended or _next_cluster.has_value(); },
                [this, &whole_log_ended] {
            _curr_cluster.align_curr_pos(_metadata_log._alignment);
            _curr_cluster.checkpoint_curr_pos();

            if (not read_and_check_checkpoint()) {
                mlogger.debug("Checkpoint invalid");
                whole_log_ended = true;
                return now();
            }

            mlogger.debug("Checkpoint correct");
            return bootstrap_checkpointed_data();
        }).then([] {
            mlogger.debug("Cluster ended");
        });
    });
}

bool metadata_log_bootstrap::read_and_check_checkpoint() {
    mlogger.debug("Processing checkpoint at {}", _curr_cluster.curr_pos());
    ondisk_type entry_type;
    ondisk_checkpoint checkpoint;
    if (not _curr_cluster.read_entry(entry_type)) {
        mlogger.debug("Cannot read entry type");
        return false;
    }
    if (entry_type != CHECKPOINT) {
        mlogger.debug("Entry type (= {}) is not CHECKPOINT (= {})", entry_type, CHECKPOINT);
        return false;
    }
    if (not _curr_cluster.read_entry(checkpoint)) {
        mlogger.debug("Cannot read checkpoint entry");
        return false;
    }

    boost::crc_32_type crc;
    if (not _curr_cluster.process_crc_without_reading(crc, checkpoint.checkpointed_data_length)) {
        mlogger.debug("Invalid checkpoint's data length: {}", (unit_size_t)checkpoint.checkpointed_data_length);
        return false;
    }
    crc.process_bytes(&checkpoint.checkpointed_data_length, sizeof(checkpoint.checkpointed_data_length));
    if (crc.checksum() != checkpoint.crc32_code) {
        mlogger.debug("CRC code does not match: computed = {}, read = {}", crc.checksum(), (uint32_t)checkpoint.crc32_code);
        return false;
    }

    auto opt = _curr_cluster.extract(checkpoint.checkpointed_data_length);
    assert(opt.has_value());
    _curr_checkpoint = *opt;
    return true;
}

future<> metadata_log_bootstrap::bootstrap_checkpointed_data() {
    return do_with(ondisk_type {}, [this](ondisk_type& entry_type) {
        return do_until([this, &entry_type] { return not _curr_checkpoint.read_entry(entry_type); },
                [this, &entry_type] {
            switch (entry_type) {
            case INVALID:
            case CHECKPOINT: // CHECKPOINT cannot appear as part of checkpointed data
                return invalid_entry_exception();
            case NEXT_METADATA_CLUSTER:
                return bootstrap_next_metadata_cluster();
            case CREATE_INODE:
                return bootstrap_create_inode();
            case DELETE_INODE:
                return bootstrap_delete_inode();
            case SMALL_WRITE:
                return bootstrap_small_write();
            case MEDIUM_WRITE:
                return bootstrap_medium_write();
            case LARGE_WRITE:
                return bootstrap_large_write();
            case LARGE_WRITE_WITHOUT_MTIME:
                return bootstrap_large_write_without_mtime();
            case TRUNCATE:
                return bootstrap_truncate();
            case ADD_DIR_ENTRY:
                return bootstrap_add_dir_entry();
            case CREATE_INODE_AS_DIR_ENTRY:
                return bootstrap_create_inode_as_dir_entry();
            case DELETE_DIR_ENTRY:
                return bootstrap_delete_dir_entry();
            case DELETE_INODE_AND_DIR_ENTRY:
                return bootstrap_delete_inode_and_dir_entry();
            }

            // unknown type => metadata log corruption
            return invalid_entry_exception();
        }).then([this] {
            if (_curr_checkpoint.bytes_left() > 0) {
                return invalid_entry_exception(); // Corrupted checkpointed data
            }
            return now();
        });
    });
}

future<> metadata_log_bootstrap::bootstrap_next_metadata_cluster() {
    ondisk_next_metadata_cluster entry;
    if (not _curr_checkpoint.read_entry(entry)) {
        return invalid_entry_exception();
    }

    if (_next_cluster.has_value()) {
        return invalid_entry_exception(); // Only one NEXT_METADATA_CLUSTER may appear in one cluster
    }

    _next_cluster = (cluster_id_t)entry.cluster_id;
    return now();
}

bool metadata_log_bootstrap::inode_exists(inode_t inode) {
    return _metadata_log._inodes.count(inode) != 0;
}

future<> metadata_log_bootstrap::bootstrap_create_inode() {
    ondisk_create_inode entry;
    if (not _curr_checkpoint.read_entry(entry) or inode_exists(entry.inode)) {
        return invalid_entry_exception();
    }

    _metadata_log.memory_only_create_inode(entry.inode, entry.is_directory,
            ondisk_metadata_to_metadata(entry.metadata));
    return now();
}

future<> metadata_log_bootstrap::bootstrap_delete_inode() {
    ondisk_delete_inode entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.inode)) {
        return invalid_entry_exception();
    }

    inode_info& inode_info = _metadata_log._inodes.at(entry.inode);
    if (inode_info.directories_containing_file > 0) {
        return invalid_entry_exception(); // Only unlinked inodes may be deleted
    }

    if (inode_info.is_directory() and not inode_info.get_directory().entries.empty()) {
        return invalid_entry_exception(); // Only empty directories may be deleted
    }

    _metadata_log.memory_only_delete_inode(entry.inode);
    return now();
}

future<> metadata_log_bootstrap::bootstrap_small_write() {
    ondisk_small_write_header entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.inode)) {
        return invalid_entry_exception();
    }

    if (not _metadata_log._inodes[entry.inode].is_file()) {
        return invalid_entry_exception();
    }

    auto data_opt = _curr_checkpoint.read_tmp_buff(entry.length);
    if (not data_opt) {
        return invalid_entry_exception();
    }
    temporary_buffer<uint8_t>& data = *data_opt;

    _metadata_log.memory_only_small_write(entry.inode, entry.offset, std::move(data));
    _metadata_log.memory_only_update_mtime(entry.inode, entry.time_ns);
    return now();
}

future<> metadata_log_bootstrap::bootstrap_medium_write() {
    ondisk_medium_write entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.inode)) {
        return invalid_entry_exception();
    }

    if (not _metadata_log._inodes[entry.inode].is_file()) {
        return invalid_entry_exception();
    }

    cluster_id_t data_cluster_id = offset_to_cluster_id(entry.disk_offset, _metadata_log._cluster_size);
    if (_available_clusters.beg > data_cluster_id or
            _available_clusters.end <= data_cluster_id) {
        return invalid_entry_exception();
    }
    // TODO: we could check overlapping with other writes
    _taken_clusters.emplace(data_cluster_id);

    _metadata_log.memory_only_disk_write(entry.inode, entry.offset, entry.disk_offset, entry.length);
    _metadata_log.memory_only_update_mtime(entry.inode, entry.time_ns);
    return now();
}

future<> metadata_log_bootstrap::bootstrap_large_write() {
    ondisk_large_write entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.inode)) {
        return invalid_entry_exception();
    }

    if (not _metadata_log._inodes[entry.inode].is_file()) {
        return invalid_entry_exception();
    }

    if (_available_clusters.beg > entry.data_cluster or
            _available_clusters.end <= entry.data_cluster or
            _taken_clusters.count(entry.data_cluster) != 0) {
        return invalid_entry_exception();
    }
    _taken_clusters.emplace((cluster_id_t)entry.data_cluster);

    _metadata_log.memory_only_disk_write(entry.inode, entry.offset,
            cluster_id_to_offset(entry.data_cluster, _metadata_log._cluster_size), _metadata_log._cluster_size);
    _metadata_log.memory_only_update_mtime(entry.inode, entry.time_ns);
    return now();
}

// TODO: copy pasting :(
future<> metadata_log_bootstrap::bootstrap_large_write_without_mtime() {
    ondisk_large_write_without_mtime entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.inode)) {
        return invalid_entry_exception();
    }

    if (not _metadata_log._inodes[entry.inode].is_file()) {
        return invalid_entry_exception();
    }

    if (_available_clusters.beg > entry.data_cluster or
            _available_clusters.end <= entry.data_cluster or
            _taken_clusters.count(entry.data_cluster) != 0) {
        return invalid_entry_exception();
    }
    _taken_clusters.emplace((cluster_id_t)entry.data_cluster);

    _metadata_log.memory_only_disk_write(entry.inode, entry.offset,
            cluster_id_to_offset(entry.data_cluster, _metadata_log._cluster_size), _metadata_log._cluster_size);
    return now();
}

future<> metadata_log_bootstrap::bootstrap_truncate() {
    ondisk_truncate entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.inode)) {
        return invalid_entry_exception();
    }

    if (not _metadata_log._inodes[entry.inode].is_file()) {
        return invalid_entry_exception();
    }

    _metadata_log.memory_only_truncate(entry.inode, entry.size);
    _metadata_log.memory_only_update_mtime(entry.inode, entry.time_ns);
    return now();
}

future<> metadata_log_bootstrap::bootstrap_add_dir_entry() {
    ondisk_add_dir_entry_header entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.dir_inode) or
            not inode_exists(entry.entry_inode)) {
        return invalid_entry_exception();
    }

    std::string dir_entry_name;
    if (not _curr_checkpoint.read_string(dir_entry_name, entry.entry_name_length)) {
        return invalid_entry_exception();
    }

    // Only files may be linked as not to create cycles (directories are created and linked using
    // CREATE_INODE_AS_DIR_ENTRY)
    if (not _metadata_log._inodes[entry.entry_inode].is_file()) {
        return invalid_entry_exception();
    }

    if (not _metadata_log._inodes[entry.dir_inode].is_directory()) {
        return invalid_entry_exception();
    }
    auto& dir = _metadata_log._inodes[entry.dir_inode].get_directory();

    if (dir.entries.count(dir_entry_name) != 0) {
        return invalid_entry_exception();
    }

    _metadata_log.memory_only_add_dir_entry(dir, entry.entry_inode, std::move(dir_entry_name));
    // TODO: Maybe mtime_ns for modifying directory?
    return now();
}

future<> metadata_log_bootstrap::bootstrap_create_inode_as_dir_entry() {
    ondisk_create_inode_as_dir_entry_header entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.dir_inode) or
            inode_exists(entry.entry_inode.inode)) {
        return invalid_entry_exception();
    }

    std::string dir_entry_name;
    if (not _curr_checkpoint.read_string(dir_entry_name, entry.entry_name_length)) {
        return invalid_entry_exception();
    }

    if (not _metadata_log._inodes[entry.dir_inode].is_directory()) {
        return invalid_entry_exception();
    }
    auto& dir = _metadata_log._inodes[entry.dir_inode].get_directory();

    if (dir.entries.count(dir_entry_name) != 0) {
        return invalid_entry_exception();
    }

    _metadata_log.memory_only_create_inode(entry.entry_inode.inode, entry.entry_inode.is_directory,
            ondisk_metadata_to_metadata(entry.entry_inode.metadata));
    _metadata_log.memory_only_add_dir_entry(dir, entry.entry_inode.inode, std::move(dir_entry_name));
    // TODO: Maybe mtime_ns for modifying directory?
    return now();
}

future<> metadata_log_bootstrap::bootstrap_delete_dir_entry() {
    ondisk_delete_dir_entry_header entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.dir_inode)) {
        return invalid_entry_exception();
    }

    std::string dir_entry_name;
    if (not _curr_checkpoint.read_string(dir_entry_name, entry.entry_name_length)) {
        return invalid_entry_exception();
    }

    if (not _metadata_log._inodes[entry.dir_inode].is_directory()) {
        return invalid_entry_exception();
    }
    auto& dir = _metadata_log._inodes[entry.dir_inode].get_directory();

    auto it = dir.entries.find(dir_entry_name);
    if (it == dir.entries.end()) {
        return invalid_entry_exception();
    }

    _metadata_log.memory_only_delete_dir_entry(dir, std::move(dir_entry_name));
    // TODO: Maybe mtime_ns for modifying directory?
    return now();
}

future<> metadata_log_bootstrap::bootstrap_delete_inode_and_dir_entry() {
    ondisk_delete_inode_and_dir_entry_header entry;
    if (not _curr_checkpoint.read_entry(entry) or not inode_exists(entry.dir_inode) or not inode_exists(entry.inode_to_delete)) {
        return invalid_entry_exception();
    }

    std::string dir_entry_name;
    if (not _curr_checkpoint.read_string(dir_entry_name, entry.entry_name_length)) {
        return invalid_entry_exception();
    }

    if (not _metadata_log._inodes[entry.dir_inode].is_directory()) {
        return invalid_entry_exception();
    }
    auto& dir = _metadata_log._inodes[entry.dir_inode].get_directory();

    auto it = dir.entries.find(dir_entry_name);
    if (it == dir.entries.end()) {
        return invalid_entry_exception();
    }

    inode_info& inode_to_delete_info = _metadata_log._inodes.at(entry.inode_to_delete);
    // We have 2 cases:
    // - deleting dir entry and inode that it points to
    // - deleting dir entry and inode that are unrelated
    // Third case: deleting dir entry and inode of this dir is not possible as we do not permit unlinked directories
    if (inode_to_delete_info.directories_containing_file > (it->second == entry.inode_to_delete)) {
        return invalid_entry_exception(); // Only unlinked inodes may be deleted
    }
    if (inode_to_delete_info.is_directory() and not inode_to_delete_info.get_directory().entries.empty()) {
        return invalid_entry_exception(); // Only empty directories may be deleted
    }

    // TODO: there is so much copy & paste above...
    // TODO: maybe to make ondisk_delete_inode_and_dir_entry_header have ondisk_delete_inode and
    //       ondisk_delete_dir_entry_header to ease deduplicating code?
    // TODO: ^ because some check above are different it may be hard here, but in other places should be fine, also
    // instead of calling bootstrap_delete_dir_entry() and then bootstrap_delete_inode() we could export
    // some (preferably all) correctness checking logic to separate functions that would allow as to reuse them, thus
    // reducing duplication to minimum

    _metadata_log.memory_only_delete_dir_entry(dir, std::move(dir_entry_name));
    _metadata_log.memory_only_delete_inode(entry.inode_to_delete);
    // TODO: Maybe mtime_ns for modifying directory?
    return now();
}

future<> metadata_log_bootstrap::bootstrap(metadata_log& metadata_log, inode_t root_dir, cluster_id_t first_metadata_cluster_id,
        cluster_range available_clusters, fs_shard_id_t fs_shards_pool_size, fs_shard_id_t fs_shard_id) {
    // Clear the metadata log
    metadata_log._inodes.clear();
    metadata_log._background_futures = now();
    metadata_log._root_dir = root_dir;
    metadata_log._inodes.emplace(root_dir, inode_info {
        0,
        0,
        {}, // TODO: change it to something meaningful
        inode_info::directory {}
    });

    return do_with(metadata_log_bootstrap(metadata_log, available_clusters),
            [first_metadata_cluster_id, fs_shards_pool_size, fs_shard_id](metadata_log_bootstrap& bootstrap) {
                return bootstrap.bootstrap(first_metadata_cluster_id, fs_shards_pool_size, fs_shard_id);
            });
}

} // namespace seastar::fs
