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

#include "seastar/core/file.hh"
#include "seastar/core/future.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/fs/bitwise.hh"
#include "seastar/fs/block_device.hh"
#include "seastar/fs/range.hh"
#include "seastar/fs/units.hh"

#include <cstdint>

namespace seastar::fs {

// Reads from unaligned offsets. Keeps simple cache to optimize consecutive reads.
class device_reader {
    block_device _device;
    unit_size_t _alignment;
    const io_priority_class& _pc;

    struct disk_data {
        disk_range _disk_range;
        temporary_buffer<uint8_t> _data;
    } _cached_disk_read;
    // _cached_disk_read keeps last disk read to accelerate next disk reads in cases where next read is intersecting
    // previous read

    struct disk_data_raw {
        disk_range _disk_range;
        uint8_t* _data;

        void trim_front(size_t count) {
            _disk_range.beg += count;
            _data += count;
        }

        void trim_back(size_t count) {
            _disk_range.end -= count;
        }

        uintptr_t data_alignment(unit_size_t alignment) {
            return mod_by_power_of_2(reinterpret_cast<uintptr_t>(_data), alignment);
        }

        size_t range_alignment(unit_size_t alignment) {
            return mod_by_power_of_2(_disk_range.beg, alignment);
        }
    };

    size_t copy_intersecting_data(disk_data_raw dest_data, const disk_data& source_data) const;

    disk_data init_aligned_disk_data(disk_range range) const;

    future<size_t> read_to_cache_and_copy_intersection(disk_range range, disk_data_raw dest_data);

    class partial_read_exception : public std::exception {};

    future<size_t> do_read(disk_data_raw disk_data_to_read);

public:
    device_reader(block_device& device, unit_size_t alignment, const io_priority_class& pc = default_priority_class())
        : _device(device)
        , _alignment(alignment)
        , _pc(pc) {}

    future<size_t> read(uint8_t* buffer, disk_offset_t disk_offset, size_t read_len);

};

} // namespace seastar::fs