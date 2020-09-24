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

#include "fs/backend/data_compaction.hh"
#include "fs/backend/inode_info.hh"
#include "fs/backend/metadata_log/entries.hh"
#include "fs/backend/write.hh"
#include "fs/device_reader.hh"
#include "seastar/core/do_with.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/core/semaphore.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/fs/bitwise.hh"
#include "seastar/fs/unit_types.hh"

#include <algorithm>
#include <boost/iterator/counting_iterator.hpp>
#include <chrono>
#include <exception>
#include <optional>
#include <stdexcept>
#include <variant>

namespace {
seastar::logger mlogger("fs_backend_data_compaction");
} // namespace

namespace seastar::fs::backend {

future<> data_compaction::compact() {
    mlogger.debug(">>>>  Started compaction  <<<<");
    mlogger.debug("source clusters: {}", _compacted_cluster_ids);

    auto allocate_compacted_data_vec = [&](disk_offset_t disk_offset,
            const data_cluster_contents_info::cluster_data_vec& data) {
        compacted_data_vec vec {
            ._data = temporary_buffer<uint8_t>::aligned(_shard._alignment,
                    round_up_to_multiple_of_power_of_2(data.data_range.size(), _shard._alignment)),
            ._inode_id = data.data_owner,
            ._file_offset = data.data_range.beg,
            ._prev_disk_offset = disk_offset,
            ._post_disk_offset = std::nullopt
        };
        vec._data.trim(data.data_range.size());
        return vec;
    };
    std::sort(_compacted_cluster_ids.begin(), _compacted_cluster_ids.end()); // TODO: verify if this is needed here or
                                                                             //       is it already done by caller
    std::vector<compacted_data_vec> data_vecs;
    for (auto& cluster_id : _compacted_cluster_ids) {
        auto cluster_it = _shard._writable_data_clusters.find(cluster_id);
        assert(cluster_it != _shard._writable_data_clusters.end()
            && "invalid cluster id, cluster doesn't store data or cluster isn't marked for compaction");
        for (auto& [disk_offset, data] : cluster_it->second.get_data()) {
            data_vecs.emplace_back(allocate_compacted_data_vec(disk_offset, data));
        }
    }

    // It is quite common to call compaction with empty clusters to immediately free those clusters
    auto compact_future = data_vecs.empty() ? now() : read_data_vectors(std::move(data_vecs));

    return compact_future.finally([this] {
        auto metadata_log_flush = _was_metadata_log_modified ? _shard.flush_curr_cluster() : now();
        return metadata_log_flush.then([this] {
            return parallel_for_each(_compacted_cluster_ids, [this](cluster_id_t cluster_id) {
                auto cluster_it = _shard._writable_data_clusters.find(cluster_id);
                assert(cluster_it != _shard._writable_data_clusters.end());
                if (cluster_it->second.is_empty()) {
                    return cluster_it->second.wait_for_all_readers_to_unlock().then(
                            [this, cluster_id, cluster_it = std::move(cluster_it)] {
                        // TODO: throw if read_only or allow it here?
                        mlogger.debug("releasing cluster: {}", cluster_id);
                        _shard.free_writable_data_cluster(cluster_id);
                    });
                } else {
                    mlogger.warn("cluster {} not empty after compaction - cannot free", cluster_id);
                    _shard.finish_writing_data_cluster(cluster_id);
                    return now();
                }
            });
        }).handle_exception([this](std::exception_ptr ptr) {
            for (auto& cluster_id : _compacted_cluster_ids) {
                _shard.finish_writing_data_cluster(cluster_id);
            }
            mlogger.warn("Exception while flushing log after compaction: cannot free compacted clusters.");
            return make_exception_future(std::move(ptr));
        });
    });
}

future<> data_compaction::read_data_vectors(std::vector<compacted_data_vec> data_vecs) {
    // TODO: We could read the whole cluster when we read more data from it than some threshold (for example
    //       more than 40%).
    return do_with(device_reader(_shard._device, _shard._alignment), std::move(data_vecs),
            [this](device_reader& reader, std::vector<compacted_data_vec>& data_vecs) {
        return do_for_each(data_vecs, [&reader](compacted_data_vec& vec) {
            return reader.read(vec._data.get_write(), vec._prev_disk_offset, vec._data.size()).then(
                    [expected_read_len = vec._data.size()](size_t read_len) {
                if (read_len != expected_read_len) {
                    return make_exception_future<>(input_output_exception());
                }
                return now();
            });
        }).then([this, &data_vecs] {
            return group_data_into_clusters(std::move(data_vecs));
        });
    });
}

future<> data_compaction::group_data_into_clusters(std::vector<compacted_data_vec> read_data_vecs) {
    _shard.throw_if_read_only_fs();

    // TODO: we could group data vecs by file and merge them to perform some defragmentation

    // First divide data into clusters and calculate data offsets in new clusters
    std::vector<std::vector<compacted_data_vec>> grouped_data_vecs;
    std::vector<compacted_data_vec> memory_data_vecs;
    {
        // TODO: We could try to avoid splitting the data using smarter strategies. That problem is similar to the multiple
        // knapsack problem where knapsacks are clusters and items are data vectors. In that case we would need
        // to remember that the main purpose of the compaction isn't smart placement of data vectors and defragmentation
        // but releasing clusters.
        bool should_add_new_group = true;
        size_t in_cluster_offset = 0;
        // For now calculate offsets of new data vecs in clusters. We will calculate final on-disk offsets after
        // clusters allocation.
        for (auto& file_data_vec : read_data_vecs) {
            size_t remaining_data_size = file_data_vec._data.size();
            size_t data_offset = 0;
            while (in_cluster_offset + remaining_data_size > _shard._cluster_size) {
                if (should_add_new_group) {
                    grouped_data_vecs.push_back({});
                    should_add_new_group = false;
                }

                size_t part_size = _shard._cluster_size - in_cluster_offset;
                grouped_data_vecs.back().push_back({
                    ._data = file_data_vec._data.share(data_offset, part_size),
                    ._inode_id = file_data_vec._inode_id,
                    ._file_offset = file_data_vec._file_offset + data_offset,
                    ._prev_disk_offset = file_data_vec._prev_disk_offset + data_offset,
                    ._post_disk_offset = in_cluster_offset
                });
                remaining_data_size -= part_size;
                data_offset += part_size;
                in_cluster_offset = 0;

                should_add_new_group = true;
            }
            if (remaining_data_size > 0) {
                size_t aligned_part_size = round_down_to_multiple_of_power_of_2(remaining_data_size, _shard._alignment);
                if (aligned_part_size > 0) {
                    if (should_add_new_group) {
                        grouped_data_vecs.emplace_back(std::vector<compacted_data_vec> {});
                        should_add_new_group = false;
                    }

                    grouped_data_vecs.back().push_back({
                        ._data = file_data_vec._data.share(data_offset, aligned_part_size),
                        ._inode_id = file_data_vec._inode_id,
                        ._file_offset = file_data_vec._file_offset + data_offset,
                        ._prev_disk_offset = file_data_vec._prev_disk_offset + data_offset,
                        ._post_disk_offset = in_cluster_offset
                    });
                    data_offset += aligned_part_size;
                    remaining_data_size -= aligned_part_size;
                    in_cluster_offset += aligned_part_size;

                    if (in_cluster_offset == _shard._cluster_size) {
                        in_cluster_offset = 0;
                        should_add_new_group = true;
                    }
                }
                if (remaining_data_size > 0) {
                    // TODO: Do we want to keep unaligned data in memory or leave it on disk?
                    memory_data_vecs.push_back({
                        ._data = file_data_vec._data.share(data_offset, remaining_data_size),
                        ._inode_id = file_data_vec._inode_id,
                        ._file_offset = file_data_vec._file_offset + data_offset,
                        ._prev_disk_offset = file_data_vec._prev_disk_offset + data_offset,
                        ._post_disk_offset = std::nullopt
                    });
                }
            }
        }
    }

    return allocate_clusters(std::move(grouped_data_vecs), std::move(memory_data_vecs));
}

future<> data_compaction::allocate_clusters(std::vector<std::vector<compacted_data_vec>> grouped_data_vecs,
        std::vector<compacted_data_vec> memory_data_vecs) {
    if (grouped_data_vecs.size() >= _compacted_cluster_ids.size()) {
        mlogger.warn("inefficient compaction: number of clusters passed for compaction: {}, number of clusters after compaction {} ",
            _compacted_cluster_ids.size(), grouped_data_vecs.size());
    }
    // Now allocate needed clusters and update offsets adding cluster beginning offset
    // TODO: we could first try to allocate clusters from shared cluster_allocater and if it fails wait for clusters
    //       from another (destined only for compactions) cluster_allocator
    size_t alloc_size = grouped_data_vecs.size();
    mlogger.debug("trying to allocate {} clusters", alloc_size);
    return _shard._cluster_allocator.alloc_wait(semaphore::duration(std::chrono::seconds(3)), alloc_size).then(
            [this, grouped_data_vecs = std::move(grouped_data_vecs), memory_data_vecs = std::move(memory_data_vecs)]
            (std::vector<cluster_id_t> cluster_ids) mutable {
        mlogger.debug("destination clusters: {} for compacted clusters {}", cluster_ids, _compacted_cluster_ids);

        for (size_t i = 0; i < cluster_ids.size(); ++i) {
            for (auto& file_data_vec : grouped_data_vecs[i]) {
                *file_data_vec._post_disk_offset += cluster_id_to_offset(cluster_ids[i], _shard._cluster_size);
            }
        }

        return save_compacted_data_vecs(std::move(cluster_ids), std::move(grouped_data_vecs), std::move(memory_data_vecs));
    }).handle_exception_type([alloc_size = alloc_size](seastar::semaphore_timed_out e) {
        mlogger.warn("Couldn't allocate {} clusters for compaction. Aborting compaction.", alloc_size);
        return make_exception_future(seastar::fs::no_more_space_exception());
    });
}

future<> data_compaction::save_compacted_data_vecs(std::vector<cluster_id_t> comp_clusters_ids,
        std::vector<std::vector<compacted_data_vec>> grouped_data_vecs,
        std::vector<compacted_data_vec> memory_data_vecs) {
    // TODO: maybe do_for_each?
    return parallel_for_each(boost::counting_iterator<size_t>(0), boost::counting_iterator<size_t>(comp_clusters_ids.size()),
            [this, grouped_data_vecs = std::move(grouped_data_vecs)](size_t i) mutable {
        return write_ondisk_data_vecs(std::move(grouped_data_vecs[i]));
    }).then([this, memory_data_vecs = std::move(memory_data_vecs)] () mutable { // TODO: maybe do write_*_data_vecs
                                                                                //       in parallel
        return write_memory_data_vecs(std::move(memory_data_vecs));
    }).handle_exception([](std::exception_ptr ptr) {
        mlogger.warn("Exception occurred after cluster allocation.");
        return make_exception_future(ptr);
    }).finally([this, comp_clusters_ids = std::move(comp_clusters_ids)] {
        for (auto& cluster_id : comp_clusters_ids) {
            auto cluster_it = _shard._writable_data_clusters.find(cluster_id);
            if (cluster_it == _shard._writable_data_clusters.end()) {
                mlogger.debug("releasing free data cluster after compaction {}", cluster_id);
                _shard._cluster_allocator.free(cluster_id);
            } else if (cluster_it->second.is_empty()) {
                mlogger.debug("releasing free data cluster after compaction {}", cluster_id);
                _shard.free_writable_data_cluster(cluster_id);
            } else {
                // Mark that cluster is after compaction
                _shard.finish_writing_data_cluster(cluster_id);
            }
        }
    });
}

future<> data_compaction::write_ondisk_data_vecs(std::vector<compacted_data_vec> file_data_vecs) {
    return do_with(std::move(file_data_vecs), [this](std::vector<compacted_data_vec>& file_data_vecs) {
        return do_for_each(file_data_vecs, [this](compacted_data_vec& vec) {
            _shard.throw_if_read_only_fs();
            return _shard._device.write(*vec._post_disk_offset, vec._data.get(), vec._data.size()).then(
                    [this, &vec](size_t write_len) {
                if (write_len == vec._data.size()) {
                    update_previous_data_vecs(vec);
                } else {
                    mlogger.debug("Partial write while writing compacted data vec to disk, skipping.");
                }
            });
        });
    });
}

future<> data_compaction::write_memory_data_vecs(std::vector<compacted_data_vec> file_data_vecs) {
    return do_with(std::move(file_data_vecs), [this](std::vector<compacted_data_vec>& file_data_vecs) {
        return do_for_each(file_data_vecs, [this](compacted_data_vec& vec) {
            update_previous_data_vecs(vec);
        });
    });
}

void data_compaction::update_previous_data_vecs(compacted_data_vec& comp_vec) {
    auto inode_it = _shard._inodes.find(comp_vec._inode_id);
    if (inode_it == _shard._inodes.end()) {
        mlogger.debug("Inode {} deleted. Skipping data vec.", comp_vec._inode_id);
        return;
    }
    assert(inode_it->second.is_file() && "Given inode doesn't refer to any file");

    inode_info& inode_info = inode_it->second;
    inode_info::file& file_info = inode_info.get_file();

    file_range comp_vec_range {comp_vec._file_offset, comp_vec._file_offset + comp_vec._data.size()};

    std::vector<inode_data_vec> prev_inode_vecs;
    file_info.execute_on_data_range(comp_vec_range, [&](inode_data_vec data_vec)  {
        prev_inode_vecs.emplace_back(std::move(data_vec));
    });
    if (!prev_inode_vecs.empty()) {
        // File was truncated and comp_vec has data before truncate. We should
        // trim data in comp_vec.
        comp_vec_range.end = prev_inode_vecs.back().data_range.end;
    }
    {
        auto move_comp_vec = [&](file_offset_t new_beg) {
            assert(new_beg > comp_vec._file_offset);
            assert(new_beg <= comp_vec_range.end);

            file_offset_t move_delta = new_beg - comp_vec._file_offset;
            comp_vec._file_offset = new_beg;
            if (comp_vec._post_disk_offset) {
                *comp_vec._post_disk_offset += move_delta;
            }
            comp_vec._data.trim_front(move_delta);
        };

        auto is_inode_vec_newer_than_comp_vec = [&](const inode_data_vec& data_vec) {
            if (!std::holds_alternative<inode_data_vec::on_disk_data>(data_vec.data_location)) {
                return true;
            }
            auto& on_disk_data = std::get<inode_data_vec::on_disk_data>(data_vec.data_location);
            return offset_to_cluster_id(on_disk_data.device_offset, _shard._cluster_size) !=
                    offset_to_cluster_id(comp_vec._prev_disk_offset, _shard._cluster_size);
        };

        namespace mle = metadata_log::entries;
        // TODO: Any suggestions on a better name for that lambda?
        auto add_write_from_comp_vec = [&](file_offset_t write_end) {
            inode_data_vec::data_location_type data_location;
            file_offset_t write_len = write_end - comp_vec._file_offset;
            if (comp_vec._post_disk_offset) {
                shard::append_result append_result;
                if (write_len == _shard._cluster_size) {
                    assert(mod_by_power_of_2(*comp_vec._post_disk_offset, _shard._cluster_size) == 0);
                    append_result = _shard.append_metadata_log(mle::large_write{
                        .lwwt = {
                            .inode = comp_vec._inode_id,
                            .offset = comp_vec._file_offset,
                            .data_cluster = offset_to_cluster_id(*comp_vec._post_disk_offset, _shard._cluster_size),
                        },
                        .time_ns = inode_info.metadata.mtime_ns
                    });
                } else {
                    append_result = _shard.append_metadata_log(mle::medium_write{
                        .inode = comp_vec._inode_id,
                        .offset = comp_vec._file_offset,
                        .drange = {
                            .beg = *comp_vec._post_disk_offset,
                            .end = *comp_vec._post_disk_offset + write_len,
                        },
                        .time_ns = inode_info.metadata.mtime_ns
                    });
                }

                switch (append_result) {
                case shard::append_result::TOO_BIG:
                case shard::append_result::NO_SPACE:
                    // TODO: we should have 'emergency' cluster for those kind of cases
                    mlogger.debug("Not enough space for on disk entry in compaction.");
                    return;
                case shard::append_result::APPENDED:
                    _was_metadata_log_modified = true;
                    // TODO: handle throws
                    _shard.memory_only_disk_write(comp_vec._inode_id, comp_vec._file_offset,
                            *comp_vec._post_disk_offset, write_len);
                    return;
                }
                __builtin_unreachable();
            } else {
                assert(write_len <= write_operation::SMALL_WRITE_THRESHOLD);
                mle::small_write entry {
                    .inode = comp_vec._inode_id,
                    .offset = comp_vec._file_offset,
                    .time_ns = inode_info.metadata.mtime_ns,
                    .data = comp_vec._data.share(0, write_len),
                };

                switch (_shard.append_metadata_log(entry)) {
                case shard::append_result::TOO_BIG:
                case shard::append_result::NO_SPACE:
                    mlogger.debug("Not enough space in metadata log for small write in compaction.");
                    return;
                case shard::append_result::APPENDED:
                    _was_metadata_log_modified = true;
                    // TODO: handle throws
                    _shard.memory_only_small_write(comp_vec._inode_id, comp_vec._file_offset,
                            std::move(entry.data));
                    return;
                }
                __builtin_unreachable();
            }
        };

        for (auto& data_vec : prev_inode_vecs) {
            // Note that data can be fragmented here so small writes on disk are possible
            // Because we don't want to fragment writes so we are delaying calling add_write_from_comp_vec() to when
            // it is necessary.
            if (is_inode_vec_newer_than_comp_vec(data_vec)) { // New data appeard - compacted data is outdated
                if (comp_vec._file_offset < data_vec.data_range.beg) {
                    // Add previously delayed writes
                    add_write_from_comp_vec(data_vec.data_range.beg);
                }
                move_comp_vec(data_vec.data_range.end); // Trim prefix with outdated data
            } else if (data_vec.data_range.end == comp_vec_range.end) {
                // Add new write for the end of the range.
                add_write_from_comp_vec(data_vec.data_range.end);
            }
        }
    }
}

} // namespace seastar::fs::backend
