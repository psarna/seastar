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

#include "seastar/core/future.hh"
#include "seastar/fs/block_device.hh"
#include "seastar/fs/file.hh"

namespace seastar::fs {

seastarfs_file_impl::seastarfs_file_impl(shared_file_handle file_handle, open_flags flags)
    : _file_handle(std::move(file_handle))
    , _open_flags(flags) {}

seastarfs_file_impl::seastarfs_file_impl(block_device dev, open_flags flags)
    : _block_device(std::move(dev))
    , _open_flags(flags) {}

future<size_t>
seastarfs_file_impl::write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) {
    return _block_device.write(pos, buffer, len, pc);
}

future<size_t>
seastarfs_file_impl::write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) {
    throw std::bad_function_call();
}

future<size_t>
seastarfs_file_impl::read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) {
    return _block_device.read(pos, buffer, len, pc);
}

future<size_t>
seastarfs_file_impl::read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) {
    throw std::bad_function_call();
}

future<>
seastarfs_file_impl::flush() {
    return _block_device.flush();
}

future<struct stat>
seastarfs_file_impl::stat() {
    throw std::bad_function_call();
}

future<>
seastarfs_file_impl::truncate(uint64_t) {
    throw std::bad_function_call();
}

future<>
seastarfs_file_impl::discard(uint64_t offset, uint64_t length) {
    throw std::bad_function_call();
}

future<>
seastarfs_file_impl::allocate(uint64_t position, uint64_t length) {
    throw std::bad_function_call();
}

future<uint64_t>
seastarfs_file_impl::size() {
    throw std::bad_function_call();
}

future<>
seastarfs_file_impl::close() noexcept {
    return _block_device.close();
}

std::unique_ptr<file_handle_impl>
seastarfs_file_impl::dup() {
    throw std::bad_function_call();
}

subscription<directory_entry>
seastarfs_file_impl::list_directory(std::function<future<> (directory_entry de)> next) {
    throw std::bad_function_call();
}

future<temporary_buffer<uint8_t>>
seastarfs_file_impl::dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) {
    throw std::bad_function_call();
}

future<file> open_file_dma(std::string name, open_flags flags) {
    return open_block_device(std::move(name)).then([flags] (block_device bd) {
        return file(make_shared<seastarfs_file_impl>(std::move(bd), flags));
    });
}

}
