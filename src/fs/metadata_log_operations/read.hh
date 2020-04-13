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

#include "fs/bitwise.hh"
#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "fs/range.hh"
#include "fs/units.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/temporary_buffer.hh"

namespace seastar::fs {

class read_operation {
    metadata_log& _metadata_log;
    inode_t _inode;
    const io_priority_class& _pc;

    read_operation(metadata_log& metadata_log, inode_t inode, const io_priority_class& pc)
        : _metadata_log(metadata_log), _inode(inode), _pc(pc) {}

    future<size_t> read(uint8_t* buffer, size_t read_len, file_offset_t file_offset) {
        auto inode_it = _metadata_log._inodes.find(_inode);
        if (inode_it == _metadata_log._inodes.end()) {
            return make_exception_future<size_t>(invalid_inode_exception());
        }
        if (inode_it->second.is_directory()) {
            return make_exception_future<size_t>(is_directory_exception());
        }
        inode_info::file* file_info = &inode_it->second.get_file();

        return _metadata_log._locks.with_lock(metadata_log::locks::shared {_inode},
                [this, file_info, buffer, read_len, file_offset] {
            // TODO: do we want to keep that lock during reading? Everything should work even after file removal
            if (not _metadata_log.inode_exists(_inode)) {
                return make_exception_future<size_t>(operation_became_invalid_exception());
            }

            // TODO: we can change it to deque to pop from data_vecs instead of iterating
            std::vector<inode_data_vec> data_vecs;
            // Extract data vectors from file_info
            file_info->execute_on_data_range({file_offset, file_offset + read_len}, [&data_vecs](inode_data_vec data_vec) {
                // TODO: for compaction: mark that clusters shouldn't be moved to _cluster_allocator before that read ends
                data_vecs.emplace_back(std::move(data_vec));
            });

            return iterate_reads(buffer, file_offset, std::move(data_vecs));
        });
    }

    future<size_t> iterate_reads(uint8_t* buffer, file_offset_t file_offset, std::vector<inode_data_vec> data_vecs) {
        return do_with(std::move(data_vecs), (size_t)0, (size_t)0,
                [this, buffer, file_offset](std::vector<inode_data_vec>& data_vecs, size_t& vec_idx, size_t& completed_read_len) {
            return repeat([this, &completed_read_len, &data_vecs, &vec_idx, buffer, file_offset] {
                if (vec_idx == data_vecs.size()) {
                    return make_ready_future<bool_class<stop_iteration_tag>>(stop_iteration::yes);
                }

                inode_data_vec& data_vec = data_vecs[vec_idx++];
                size_t expected_read_len = data_vec.data_range.size();

                return do_read(data_vec, buffer + data_vec.data_range.beg - file_offset).then(
                        [&completed_read_len, expected_read_len](size_t read_len) {
                    completed_read_len += read_len;
                    if (read_len != expected_read_len) {
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                });
            }).then([&completed_read_len] {
                return make_ready_future<size_t>(completed_read_len);
            });
        });
    }

    struct disk_temp_buffer {
        disk_range _disk_range;
        temporary_buffer<uint8_t> _data;
    };
    // Keep last disk read to accelerate next disk reads in cases where next read is intersecting previous disk read
    // (after alignment)
    disk_temp_buffer _prev_disk_read;

    future<size_t> do_read(inode_data_vec& data_vec, uint8_t* buffer) {
        size_t expected_read_len = data_vec.data_range.size();

        return std::visit(overloaded {
            [&](inode_data_vec::in_mem_data& mem) {
                std::memcpy(buffer, mem.data.get(), expected_read_len);
                return make_ready_future<size_t>(expected_read_len);
            },
            [&](inode_data_vec::hole_data&) {
                std::memset(buffer, 0, expected_read_len);
                return make_ready_future<size_t>(expected_read_len);
            },
            [&](inode_data_vec::on_disk_data& disk_data) {
                return do_disk_read({
                        disk_data.device_offset,
                        disk_data.device_offset + expected_read_len
                    }, buffer);
            },
        }, data_vec.data_location);
    }

    future<size_t> do_disk_read(disk_range read_range, uint8_t* buffer_tmp) {
        size_t oper_read_len = read_range.size();
        lw_shared_ptr<size_t> current_read_len = make_lw_shared((size_t)0);
        lw_shared_ptr<disk_range> remaining_read_range = make_lw_shared(read_range);
        lw_shared_ptr<uint8_t*> buffer = make_lw_shared(buffer_tmp);

        // Copies data from source_buffer corresponding to the intersection of dest_disk_range
        // and source_buffer.disk_range into buffer.
        auto copy_intersecting_data =
                [](uint8_t* buffer, disk_range dest_disk_range, const disk_temp_buffer& source_buffer) -> size_t {
            disk_range intersect = intersection(dest_disk_range, source_buffer._disk_range);

            if (intersect.is_empty()) {
                return 0;
            }

            disk_offset_t common_data_len = intersect.size();
            if (dest_disk_range.beg >= source_buffer._disk_range.beg) {
                // Source data starts before dest data
                disk_offset_t source_data_offset = dest_disk_range.beg - source_buffer._disk_range.beg;
                // TODO: maybe we should split that memcpy to multiple parts because large reads can lead
                // to spikes in latency
                std::memcpy(buffer, source_buffer._data.get() + source_data_offset, common_data_len);
            } else {
                // Source data starts after dest data
                disk_offset_t dest_data_offset = source_buffer._disk_range.beg - dest_disk_range.beg;
                std::memcpy(buffer + dest_data_offset, source_buffer._data.get(), common_data_len);
            }
            return common_data_len;
        };

        if (not _prev_disk_read._data.empty()) {
            *current_read_len = copy_intersecting_data(*buffer, *remaining_read_range, _prev_disk_read);
            if (*current_read_len == oper_read_len) {
                return make_ready_future<size_t>(oper_read_len);
            }
            remaining_read_range->beg += *current_read_len;
            *buffer += *current_read_len;
        }

        auto init_aligned_disk_temp_buffer = [&](disk_range range) {
            disk_temp_buffer disk_read;
            disk_read._disk_range = {
                round_down_to_multiple_of_power_of_2(range.beg, _metadata_log._alignment),
                round_up_to_multiple_of_power_of_2(range.end, _metadata_log._alignment)
            };
            disk_read._data = temporary_buffer<uint8_t>::aligned(_metadata_log._alignment, disk_read._disk_range.size());
            return disk_read;
        };

        if (mod_by_power_of_2(reinterpret_cast<uintptr_t>(*buffer), _metadata_log._alignment) ==
                mod_by_power_of_2(remaining_read_range->beg, _metadata_log._alignment)) {
            _prev_disk_read = init_aligned_disk_temp_buffer({remaining_read_range->beg, remaining_read_range->beg});

            class partial_read_exception : public std::exception {};

            // Read prefix
            size_t expected_read_len = _prev_disk_read._disk_range.size();
            return _metadata_log._device.read(_prev_disk_read._disk_range.beg, _prev_disk_read._data.get_write(),
                    expected_read_len, _pc).then(
                    [this, buffer, copy_intersecting_data, remaining_read_range, current_read_len, expected_read_len](size_t read_len) {
                bool partial_read = read_len != expected_read_len;

                _prev_disk_read._disk_range.end = _prev_disk_read._disk_range.beg + read_len;

                auto copied_data = copy_intersecting_data(*buffer, *remaining_read_range, _prev_disk_read);
                remaining_read_range->beg += copied_data;
                *current_read_len += copied_data;
                *buffer += copied_data;

                if (partial_read) {
                    // Don't read more in case of partial read
                    return make_exception_future<>(partial_read_exception());
                }

                return now();
            }).then([this, remaining_read_range, current_read_len, buffer] {
                // Read middle part (directly into the output buffer)
                if (remaining_read_range->is_empty()) {
                    return now();
                } else {
                    size_t expected_read_len = round_down_to_multiple_of_power_of_2(remaining_read_range->size(), _metadata_log._alignment);
                    assert(remaining_read_range->beg % _metadata_log._alignment == 0);
                    return _metadata_log._device.read(remaining_read_range->beg, *buffer, expected_read_len, _pc).then(
                            [buffer, remaining_read_range, current_read_len, expected_read_len](size_t read_len) {
                        *current_read_len += read_len;
                        remaining_read_range->beg += read_len;
                        *buffer += read_len;

                        if (read_len != expected_read_len) {
                            // Partial read - end here
                            return make_exception_future<>(partial_read_exception());
                        }

                        return now();
                    });
                }
            }).then([this, remaining_read_range, current_read_len, buffer, init_aligned_disk_temp_buffer, copy_intersecting_data] {
                if (remaining_read_range->is_empty()) {
                    return make_ready_future<size_t>(*current_read_len);
                } else {
                    _prev_disk_read = init_aligned_disk_temp_buffer(*remaining_read_range);
                    assert(_prev_disk_read._disk_range.size() == _metadata_log._alignment);

                    return _metadata_log._device.read(_prev_disk_read._disk_range.beg, _prev_disk_read._data.get_write(),
                            _prev_disk_read._disk_range.size(), _pc).then(
                            [this, copy_intersecting_data, remaining_read_range, buffer, current_read_len](size_t read_len) {
                        _prev_disk_read._disk_range.end = _prev_disk_read._disk_range.beg + read_len;
                        auto copied_data = copy_intersecting_data(*buffer, *remaining_read_range, _prev_disk_read);
                        return *current_read_len + copied_data;
                    });
                }
            }).handle_exception_type([current_read_len] (partial_read_exception&) {
                return *current_read_len;
            });
        } else {
            _prev_disk_read = init_aligned_disk_temp_buffer(*remaining_read_range);

            return _metadata_log._device.read(_prev_disk_read._disk_range.beg, _prev_disk_read._data.get_write(),
                    _prev_disk_read._disk_range.size(), _pc).then(
                    [this, copy_intersecting_data, remaining_read_range, buffer, current_read_len](size_t read_len) {
                _prev_disk_read._disk_range.end = _prev_disk_read._disk_range.beg + read_len;
                auto copied_data = copy_intersecting_data(*buffer, *remaining_read_range, _prev_disk_read);
                return *current_read_len + copied_data;
            });
        }
    }

public:
    static future<size_t> perform(metadata_log& metadata_log, inode_t inode, file_offset_t pos, void* buffer,
            size_t len, const io_priority_class& pc) {
        return do_with(read_operation(metadata_log, inode, pc), [pos, buffer, len](auto& obj) {
            return obj.read(static_cast<uint8_t*>(buffer), len, pos);
        });
    }
};

} // namespace seastar::fs
