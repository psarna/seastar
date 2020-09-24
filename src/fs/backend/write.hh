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

#include "fs/backend/inode_info.hh"
#include "fs/backend/metadata_log/entries.hh"
#include "fs/backend/shard.hh"
#include "fs/cluster_utils.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/fs/bitwise.hh"
#include "seastar/fs/unit_types.hh"

namespace seastar::fs::backend {

class write_operation {
public:
    // TODO: decide about threshold for small write
    static constexpr size_t SMALL_WRITE_THRESHOLD = 8192;

private:
    shard& _shard;
    inode_t _inode;
    const io_priority_class& _pc;

    write_operation(shard& shard, inode_t inode, const io_priority_class& pc)
        : _shard(shard), _inode(inode), _pc(pc) {
        assert(_shard._alignment <= SMALL_WRITE_THRESHOLD &&
                "Small write threshold should be at least as big as alignment");
    }

    future<size_t> write(const uint8_t* buffer, size_t write_len, file_offset_t file_offset) {
        auto inode_it = _shard._inodes.find(_inode);
        if (inode_it == _shard._inodes.end()) {
            return make_exception_future<size_t>(invalid_inode_exception());
        }
        if (inode_it->second.is_directory()) {
            return make_exception_future<size_t>(is_directory_exception());
        }

        // TODO: maybe check if there is enough free clusters before executing?
        return _shard._locks.with_lock(shard::locks::shared{_inode}, [this, buffer, write_len, file_offset] {
            if (!_shard.inode_exists(_inode)) {
                return make_exception_future<size_t>(operation_became_invalid_exception());
            }
            return iterate_writes(buffer, write_len, file_offset);
        });
    }

    future<size_t> iterate_writes(const uint8_t* buffer, size_t write_len, file_offset_t file_offset) {
        return do_with((size_t)0, [this, buffer, write_len, file_offset](size_t& completed_write_len) {
            return repeat([this, &completed_write_len, buffer, write_len, file_offset] {
                if (completed_write_len == write_len) {
                    return make_ready_future<bool_class<stop_iteration_tag>>(stop_iteration::yes);
                }

                size_t remaining_write_len = write_len - completed_write_len;

                size_t expected_write_len;
                if (remaining_write_len <= SMALL_WRITE_THRESHOLD) {
                    expected_write_len = remaining_write_len;
                } else {
                    if (auto buffer_alignment = mod_by_power_of_2(reinterpret_cast<uintptr_t>(buffer) + completed_write_len,
                            _shard._alignment); buffer_alignment != 0) {
                        // When buffer is not aligned then align it using one small write
                        expected_write_len = _shard._alignment - buffer_alignment;
                    } else {
                        if (remaining_write_len >= _shard._cluster_size) {
                            expected_write_len = _shard._cluster_size;
                        } else {
                            // If the last write is medium then align write length by splitting last write into medium aligned
                            // write and small write
                            expected_write_len = remaining_write_len;
                        }
                    }
                }

                auto shifted_buffer = buffer + completed_write_len;
                auto shifted_file_offset = file_offset + completed_write_len;
                auto write_future = make_ready_future<size_t>(0);
                if (expected_write_len <= SMALL_WRITE_THRESHOLD) {
                    write_future = do_small_write(shifted_buffer, expected_write_len, shifted_file_offset);
                } else if (expected_write_len < _shard._cluster_size) {
                    write_future = medium_write(shifted_buffer, expected_write_len, shifted_file_offset);
                } else {
                    // Update mtime only when it is the first write
                    write_future = do_large_write(shifted_buffer, shifted_file_offset, completed_write_len == 0);
                }

                return write_future.then([&completed_write_len, expected_write_len](size_t write_len) {
                    completed_write_len += write_len;
                    if (write_len != expected_write_len) {
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                });
            }).then([&completed_write_len] {
                return make_ready_future<size_t>(completed_write_len);
            });
        });
    }

    future<size_t> do_small_write(const uint8_t* buffer, size_t expected_write_len, file_offset_t file_offset) {
        metadata_log::entries::small_write entry = {
            .inode = _inode,
            .offset = file_offset,
            .time_ns = _shard._clock->current_time_ns(),
            .data = temporary_buffer<uint8_t>(buffer, expected_write_len),
        };

        switch (_shard.append_metadata_log(entry)) {
        case shard::append_result::TOO_BIG:
            return make_exception_future<size_t>(cluster_size_too_small_to_perform_operation_exception());
        case shard::append_result::NO_SPACE:
            return make_exception_future<size_t>(no_more_space_exception());
        case shard::append_result::APPENDED:
            _shard.memory_only_small_write(_inode, file_offset, std::move(entry.data));
            _shard.memory_only_update_mtime(_inode, entry.time_ns);
            return make_ready_future<size_t>(expected_write_len);
        }
        __builtin_unreachable();
    }

    future<size_t> medium_write(const uint8_t* aligned_buffer, size_t expected_write_len, file_offset_t file_offset) {
        assert(reinterpret_cast<uintptr_t>(aligned_buffer) % _shard._alignment == 0);
        // TODO: medium write can be divided into bigger number of smaller writes. Maybe we should add checks
        // for that and allow only limited number of medium writes? Or we could add to to_disk_buffer option for
        // space 'reservation' to make sure that after division our write will fit into the buffer?
        // That would also limit medium write to at most two smaller writes.
        return do_with((size_t)0, [this, aligned_buffer, expected_write_len, file_offset](size_t& completed_write_len) {
            return repeat([this, &completed_write_len, aligned_buffer, expected_write_len, file_offset] {
                if (completed_write_len == expected_write_len) {
                    return make_ready_future<bool_class<stop_iteration_tag>>(stop_iteration::yes);
                }

                size_t remaining_write_len = expected_write_len - completed_write_len;
                size_t rounded_remaining_write_len =
                        round_down_to_multiple_of_power_of_2(remaining_write_len, _shard._alignment);
                size_t curr_expected_write_len;
                auto shifted_buffer = aligned_buffer + completed_write_len;
                auto shifted_file_offset = file_offset + completed_write_len;
                auto write_future = make_ready_future<size_t>(0);
                if (remaining_write_len <= SMALL_WRITE_THRESHOLD) {
                    // We can use small write for the remaining data
                    curr_expected_write_len = remaining_write_len;
                    write_future = do_small_write(shifted_buffer, curr_expected_write_len, shifted_file_offset);
                } else if (rounded_remaining_write_len <= SMALL_WRITE_THRESHOLD) {
                    curr_expected_write_len = rounded_remaining_write_len;
                    write_future = do_small_write(shifted_buffer, curr_expected_write_len, shifted_file_offset);
                } else {
                    // We must use medium write
                    size_t buff_bytes_left = _shard._medium_data_log_cw->bytes_left();
                    if (buff_bytes_left <= SMALL_WRITE_THRESHOLD) {
                        // TODO: empty clusters compaction: we could check here if current data cluster has no up-to-date
                        //       data. If so we could schedule compaction of that cluster in order to immediately move it
                        //       to _cluster_allocator.
                        // TODO: add wasted buff_bytes_left bytes for compaction
                        // No space left in the current to_disk_buffer for medium write - allocate a new buffer

                        _shard.throw_if_read_only_fs();
                        std::optional<cluster_id_t> cluster_opt = _shard._cluster_allocator.alloc();
                        if (!cluster_opt) {
                            // TODO: maybe we should return partial write instead of exception?
                            return make_exception_future<bool_class<stop_iteration_tag>>(no_more_space_exception());
                        }
                        auto finished_cluster_offset = _shard._medium_data_log_cw->initial_disk_offset();
                        auto finished_cluster_id = offset_to_cluster_id(finished_cluster_offset, _shard._cluster_size);
                        if (_shard._medium_data_log_cw->get_use_counter() == 0) {
                            _shard.finish_writing_data_cluster(finished_cluster_id);
                        }

                        auto cluster_id = cluster_opt.value();
                        disk_offset_t cluster_disk_offset = cluster_id_to_offset(cluster_id, _shard._cluster_size);
                        _shard._medium_data_log_cw = _shard._medium_data_log_cw->virtual_constructor();
                        _shard._medium_data_log_cw->init(_shard._cluster_size, _shard._alignment,
                                cluster_disk_offset);
                        buff_bytes_left = _shard._medium_data_log_cw->bytes_left();

                        curr_expected_write_len = rounded_remaining_write_len;
                    } else {
                        // There is enough space for medium write
                        curr_expected_write_len = buff_bytes_left >= rounded_remaining_write_len ?
                                rounded_remaining_write_len : buff_bytes_left;
                    }

                    write_future = do_medium_write(shifted_buffer, curr_expected_write_len, shifted_file_offset,
                            _shard._medium_data_log_cw);
                }

                return write_future.then([&completed_write_len, curr_expected_write_len](size_t write_len) {
                    completed_write_len += write_len;
                    if (write_len != curr_expected_write_len) {
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                });
            }).then([&completed_write_len] {
                return make_ready_future<size_t>(completed_write_len);
            });;
        });
    }

    future<size_t> do_medium_write(const uint8_t* aligned_buffer, size_t aligned_expected_write_len, file_offset_t file_offset,
            shared_ptr<cluster_writer> data_writer) {
        // TODO: rename data_writer to something more meaningful i.e. corresponding with _shard._medium_data_log_cw
        assert(reinterpret_cast<uintptr_t>(aligned_buffer) % _shard._alignment == 0);
        assert(aligned_expected_write_len % _shard._alignment == 0);
        assert(data_writer->bytes_left() >= aligned_expected_write_len);

        _shard.throw_if_read_only_fs();

        disk_offset_t device_offset = data_writer->current_disk_offset();
        data_writer->change_use_counter(1);
        return data_writer->write(aligned_buffer, aligned_expected_write_len, _shard._device).then(
                [this, file_offset, data_writer, device_offset](size_t write_len) {
            // TODO: is this round down necessary?
            // On partial write return aligned write length
            write_len = round_down_to_multiple_of_power_of_2(write_len, _shard._alignment);

            metadata_log::entries::medium_write entry = {
                .inode = _inode,
                .offset = file_offset,
                .drange = {
                    .beg = device_offset,
                    .end = device_offset + write_len,
                },
                .time_ns = _shard._clock->current_time_ns(),
            };

            switch (_shard.append_metadata_log(entry)) {
            case shard::append_result::TOO_BIG:
                return make_exception_future<size_t>(cluster_size_too_small_to_perform_operation_exception());
            case shard::append_result::NO_SPACE:
                return make_exception_future<size_t>(no_more_space_exception());
            case shard::append_result::APPENDED:
                _shard.memory_only_disk_write(_inode, file_offset, device_offset, write_len);
                _shard.memory_only_update_mtime(_inode, entry.time_ns);
                return make_ready_future<size_t>(write_len);
            }
            __builtin_unreachable();
        }).finally([this, data_writer] {
            data_writer->change_use_counter(-1);
            if (data_writer->get_use_counter() == 0 && _shard._medium_data_log_cw->initial_disk_offset() !=
                    data_writer->initial_disk_offset()) {
                _shard.finish_writing_data_cluster(
                        offset_to_cluster_id(data_writer->initial_disk_offset(), _shard._cluster_size));
            }
        });
    }

    future<size_t> do_large_write(const uint8_t* aligned_buffer, file_offset_t file_offset, bool update_mtime) {
        assert(reinterpret_cast<uintptr_t>(aligned_buffer) % _shard._alignment == 0);

        _shard.throw_if_read_only_fs();

        // aligned_expected_write_len = _shard._cluster_size
        std::optional<cluster_id_t> cluster_opt = _shard._cluster_allocator.alloc();
        if (!cluster_opt) {
            return make_exception_future<size_t>(no_more_space_exception());
        }
        auto cluster_id = cluster_opt.value();
        disk_offset_t cluster_disk_offset = cluster_id_to_offset(cluster_id, _shard._cluster_size);

        return _shard._device.write(cluster_disk_offset, aligned_buffer, _shard._cluster_size, _pc).then(
                [this, file_offset, cluster_id, cluster_disk_offset, update_mtime](size_t write_len) {
            if (write_len != _shard._cluster_size) {
                return make_ready_future<size_t>(0);
            }

            namespace mle = metadata_log::entries;
            shard::append_result append_result;
            {
                mle::large_write_without_time lwwt = {
                    .inode = _inode,
                    .offset = file_offset,
                    .data_cluster = cluster_id,
                };
                if (update_mtime) {
                    mle::large_write entry = {
                        .lwwt = lwwt,
                        .time_ns = _shard._clock->current_time_ns(),
                    };
                    append_result = _shard.append_metadata_log(entry);
                    if (append_result == shard::append_result::APPENDED) {
                        _shard.memory_only_update_mtime(_inode, entry.time_ns);
                    }
                } else {
                    append_result = _shard.append_metadata_log(lwwt);
                }
            }

            switch (append_result) {
            case shard::append_result::TOO_BIG:
                return make_exception_future<size_t>(cluster_size_too_small_to_perform_operation_exception());
            case shard::append_result::NO_SPACE:
                return make_exception_future<size_t>(no_more_space_exception());
            case shard::append_result::APPENDED:
                _shard.memory_only_disk_write(_inode, file_offset, cluster_disk_offset, write_len);
                _shard.finish_writing_data_cluster(cluster_id);
                return make_ready_future<size_t>(write_len);
            }
            __builtin_unreachable();
        }).then([this, cluster_id](size_t write_len) -> size_t {
            if (write_len != _shard._cluster_size) {
                _shard._cluster_allocator.free(cluster_id);
                return 0;
            }
            return write_len;
        }).handle_exception([this, cluster_id](std::exception_ptr ptr) {
            _shard._cluster_allocator.free(cluster_id);
            return make_exception_future<size_t>(std::move(ptr));
        });
    }

public:
    static future<size_t> perform(shard& shard, inode_t inode, file_offset_t pos, const void* buffer,
            size_t len, const io_priority_class& pc) {
        return do_with(write_operation(shard, inode, pc), [buffer, len, pos](auto& obj) {
            return obj.write(static_cast<const uint8_t*>(buffer), len, pos);
        });
    }
};

} // namespace seastar::fs::backend
