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

#include "fs/cluster_writer.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/fs/block_device.hh>

#include <cassert>
#include <cstdlib>
#include <vector>

namespace seastar::fs {

class mock_cluster_writer : public cluster_writer {
public:
    mock_cluster_writer() = default;

    // A container with all the writers created by virtual_constructor
    inline static thread_local std::vector<shared_ptr<mock_cluster_writer>> virtually_constructed_writers;

    shared_ptr<cluster_writer> virtual_constructor() const override {
        auto new_writer = make_shared<mock_cluster_writer>();
        virtually_constructed_writers.emplace_back(new_writer);
        return new_writer;
    }

    struct write_to_device {
        disk_offset_t disk_offset;
        temporary_buffer<uint8_t> data;
    };

    std::vector<write_to_device> writes;

    using cluster_writer::init;
    using cluster_writer::bytes_left;
    using cluster_writer::current_disk_offset;

    future<size_t> write(const void* aligned_buffer, size_t aligned_len, block_device device) override {
        assert(reinterpret_cast<uintptr_t>(aligned_buffer) % _alignment == 0);
        assert(aligned_len % _alignment == 0);
        assert(aligned_len <= bytes_left());

        writes.emplace_back(write_to_device {
            _cluster_beg_offset + _next_write_offset,
            temporary_buffer<uint8_t>(static_cast<const uint8_t*>(aligned_buffer), aligned_len)
        });

        size_t curr_write_offset = _next_write_offset;
        _next_write_offset += aligned_len;

        return device.write(_cluster_beg_offset + curr_write_offset, aligned_buffer, aligned_len);
    }

};

} // namespace seastar::fs
