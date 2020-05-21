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
 * Copyright (C) 2019 ScyllaDB
 */

#pragma once

#include "seastar/fs/units.hh"
#include "seastar/core/file.hh"
#include "seastar/core/reactor.hh"

namespace seastar::fs {

class block_device_impl {
public:
    virtual ~block_device_impl() = default;

    virtual future<size_t> write(uint64_t aligned_pos, const void* aligned_buffer, size_t aligned_len, const io_priority_class& pc) = 0;
    virtual future<size_t> read(uint64_t aligned_pos, void* aligned_buffer, size_t aligned_len, const io_priority_class& pc) = 0;
    virtual future<> flush() = 0;
    virtual future<disk_offset_t> size() = 0;
    virtual future<> close() = 0;
};

class block_device {
    shared_ptr<block_device_impl> _block_device_impl;
public:
    block_device(shared_ptr<block_device_impl> impl) noexcept : _block_device_impl(std::move(impl)) {}

    block_device() = default;

    block_device(const block_device&) = default;
    block_device(block_device&&) noexcept = default;
    block_device& operator=(const block_device&) noexcept = default;
    block_device& operator=(block_device&&) noexcept = default;

    explicit operator bool() const noexcept { return bool(_block_device_impl); }

    template <typename CharType>
    future<size_t> read(uint64_t aligned_offset, CharType* aligned_buffer, size_t aligned_len, const io_priority_class& pc = default_priority_class()) {
        return _block_device_impl->read(aligned_offset, aligned_buffer, aligned_len, pc);
    }

    template <typename CharType>
    future<size_t> write(uint64_t aligned_offset, const CharType* aligned_buffer, size_t aligned_len, const io_priority_class& pc = default_priority_class()) {
        return _block_device_impl->write(aligned_offset, aligned_buffer, aligned_len, pc);
    }

    future<> flush() {
        return _block_device_impl->flush();
    }

    future<disk_offset_t> size() {
        return _block_device_impl->size();
    };

    future<> close() {
        return _block_device_impl->close();
    }
};

class file_block_device_impl : public block_device_impl {
    file _file;
public:
    explicit file_block_device_impl(file f) : _file(std::move(f)) {}

   ~file_block_device_impl() override = default;

    future<size_t> write(uint64_t aligned_pos, const void* aligned_buffer, size_t aligned_len, const io_priority_class& pc) override {
        return _file.dma_write(aligned_pos, aligned_buffer, aligned_len, pc);
    }

    future<size_t> read(uint64_t aligned_pos, void* aligned_buffer, size_t aligned_len, const io_priority_class& pc) override {
        return _file.dma_read(aligned_pos, aligned_buffer, aligned_len, pc);
    }

    future<> flush() override {
        return _file.flush();
    }

    future<disk_offset_t> size() override {
        return _file.size();
    };

    future<> close() override {
        return _file.close();
    }
};

inline future<block_device> open_block_device(std::string name) {
    return open_file_dma(std::move(name), open_flags::rw).then([](file f) {
        return block_device(make_shared<file_block_device_impl>(std::move(f)));
    });
}

}
