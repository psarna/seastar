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

#include "seastar/core/shared_ptr.hh"
#include "seastar/fs/bitwise.hh"
#include "seastar/fs/block_device.hh"
#include "seastar/fs/unit_types.hh"

#include <cassert>
#include <cstdlib>

namespace seastar::fs::backend {

// Represents buffer that will be written to a block_device. Method init() should be called just after constructor
// in order to finish construction.
class cluster_writer {
protected:
    size_t _max_size = 0;
    disk_offset_t _alignment = 0;
    disk_offset_t _cluster_beg_offset = 0;
    size_t _next_write_offset = 0;
    size_t _use_cnt = 0;
public:
    cluster_writer() = default;

    virtual shared_ptr<cluster_writer> virtual_constructor() const {
        return make_shared<cluster_writer>();
    }

    // Total number of bytes appended cannot exceed @p aligned_max_size.
    // @p cluster_beg_offset is the disk offset of the beginning of the cluster.
    virtual void init(size_t aligned_max_size, disk_offset_t alignment, disk_offset_t cluster_beg_offset) {
        assert(is_power_of_2(alignment));
        assert(mod_by_power_of_2(aligned_max_size, alignment) == 0);
        assert(mod_by_power_of_2(cluster_beg_offset, alignment) == 0);

        _max_size = aligned_max_size;
        _alignment = alignment;
        _cluster_beg_offset = cluster_beg_offset;
        _next_write_offset = 0;
    }

    // Writes @p aligned_buffer to @p device just after previous write (or at @p cluster_beg_offset passed to init()
    // if it is the first write).
    virtual future<size_t> write(const void* aligned_buffer, size_t aligned_len, block_device device) {
        assert(reinterpret_cast<uintptr_t>(aligned_buffer) % _alignment == 0);
        assert(aligned_len % _alignment == 0);
        assert(aligned_len <= bytes_left());

        // Make sure the writer is usable before returning from this function
        size_t curr_write_offset = _next_write_offset;
        _next_write_offset += aligned_len;

        return device.write(_cluster_beg_offset + curr_write_offset, aligned_buffer, aligned_len);
    }

    virtual size_t bytes_left() const noexcept { return _max_size - _next_write_offset; }

    // Returns disk offset of the place where the first byte of next appended bytes would be after flush
    // TODO: maybe better name for that function? Or any other method to extract that data?
    virtual disk_offset_t current_disk_offset() const noexcept {
        return _cluster_beg_offset + _next_write_offset;
    }

    disk_offset_t initial_disk_offset() const noexcept {
        return _cluster_beg_offset;
    }

    void change_use_counter(ssize_t num) noexcept {
        _use_cnt += num;
    }

    ssize_t get_use_counter() const noexcept {
        return _use_cnt;
    }
};

} // namespace seastar::fs::backend
