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
 * Copyright (C) 2020 ScyllaDB Ltd.
 */

#pragma once

#include <seastar/fs/block_device.hh>
#include <seastar/fs/unit_types.hh>
#include <string>

namespace seastar::fs {

class block_device_mocker_impl : public block_device_impl {
public:
    using buf_type = std::basic_string<uint8_t>;

    size_t alignment;
    buf_type buf;
    struct write_operation {
        uint64_t disk_offset;
        temporary_buffer<uint8_t> data;
    };

    std::vector<write_operation> writes;

    ~block_device_mocker_impl() override = default;

    block_device_mocker_impl() = delete;

    explicit block_device_mocker_impl(size_t alignment) : alignment(alignment) {}

    future<size_t> write(uint64_t pos, const void* buffer, size_t len, const io_priority_class&) override;

    future<size_t> read(uint64_t pos, void* buffer, size_t len, const io_priority_class&) noexcept override;

    future<> flush() noexcept override {
        return make_ready_future<>();
    }

    future<disk_offset_t> size() noexcept override {
        return make_ready_future<disk_offset_t>(buf.size());
    }

    future<> close() noexcept override {
        return make_ready_future<>();
    }
};

inline block_device block_device_mocker(size_t alignment) {
    return block_device(make_shared<block_device_mocker_impl>(alignment));
}

} // namespace seastar::fs
