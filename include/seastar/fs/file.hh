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

#include "fs/file_handle.hh"

#include "seastar/core/file.hh"
#include "seastar/core/future.hh"
#include "seastar/fs/block_device.hh"

namespace seastar::fs {

class seastarfs_file_impl : public file_impl {
    shared_file_handle _file_handle;
    block_device _block_device;
    open_flags _open_flags;
public:
    seastarfs_file_impl(shared_file_handle file_handle, open_flags flags);
    seastarfs_file_impl(block_device dev, open_flags flags);
    ~seastarfs_file_impl() override = default;

    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override;
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override;
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override;
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override;
    future<> flush() override;
    future<struct stat> stat() override;
    future<> truncate(uint64_t length) override;
    future<> discard(uint64_t offset, uint64_t length) override;
    future<> allocate(uint64_t position, uint64_t length) override;
    future<uint64_t> size() override;
    future<> close() noexcept override;
    std::unique_ptr<file_handle_impl> dup() override;
    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override;
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override;
};

future<file> open_file_dma(std::string name, open_flags flags);

}
