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

#include "fs/inode.hh"
#include "fs/inode_info.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/metadata_log.hh"
#include "fs/range.hh"
#include "fs/units.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
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
                // TODO: we can optimize the case when disk_data.device_offset is aligned

                // Copies data from source_buffer corresponding to the intersection of dest_disk_range
                // and source_buffer.disk_range into buffer. dest_disk_range.beg corresponds to first byte of buffer
                // Works when dest_disk_range.beg <= source_buffer._disk_range.beg
                auto copy_left_intersecting_data =
                        [](uint8_t* buffer, disk_range dest_disk_range, const disk_temp_buffer& source_buffer) -> size_t {
                    disk_range intersect = intersection(dest_disk_range, source_buffer._disk_range);

                    assert((intersect.is_empty() or dest_disk_range.beg >= source_buffer._disk_range.beg) and
                            "Beggining of source buffer on disk should be before beggining of destination buffer on disk");

                    if (not intersect.is_empty()) {
                        // We can copy _data from disk_temp_buffer
                        disk_offset_t common_data_len = intersect.size();
                        disk_offset_t source_data_offset = dest_disk_range.beg - source_buffer._disk_range.beg;
                        // TODO: maybe we should split that memcpy to multiple parts because large reads can lead
                        // to spikes in latency
                        std::memcpy(buffer, source_buffer._data.get() + source_data_offset, common_data_len);
                        return common_data_len;
                    } else {
                        return 0;
                    }
                };

                disk_range remaining_read_range {
                    disk_data.device_offset,
                    disk_data.device_offset + expected_read_len
                };

                size_t current_read_len = 0;
                if (not _prev_disk_read._data.empty()) {
                    current_read_len = copy_left_intersecting_data(buffer, remaining_read_range, _prev_disk_read);
                    if (current_read_len == expected_read_len) {
                        return make_ready_future<size_t>(expected_read_len);
                    }
                    remaining_read_range.beg += current_read_len;
                    buffer += current_read_len;
                }

                disk_temp_buffer new_disk_read;
                new_disk_read._disk_range = {
                    round_down_to_multiple_of_power_of_2(remaining_read_range.beg, _metadata_log._alignment),
                    round_up_to_multiple_of_power_of_2(remaining_read_range.end, _metadata_log._alignment)
                };
                new_disk_read._data = temporary_buffer<uint8_t>::aligned(_metadata_log._alignment, new_disk_read._disk_range.size());

                return _metadata_log._device.read(new_disk_read._disk_range.beg, new_disk_read._data.get_write(),
                        new_disk_read._disk_range.size(), _pc).then(
                        [this, copy_left_intersecting_data = std::move(copy_left_intersecting_data),
                        new_disk_read = std::move(new_disk_read),
                        remaining_read_range, buffer, current_read_len](size_t read_len) mutable {
                    new_disk_read._disk_range.end = new_disk_read._disk_range.beg + read_len;
                    current_read_len += copy_left_intersecting_data(buffer, remaining_read_range, new_disk_read);
                    _prev_disk_read = std::move(new_disk_read);
                    return current_read_len;
                });
            },
        }, data_vec.data_location);
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
