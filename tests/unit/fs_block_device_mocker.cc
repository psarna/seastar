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

#include "fs_block_device_mocker.hh"

#include <boost/test/unit_test.hpp>

namespace seastar::fs {

namespace {
logger mlogger("fs_block_device_mocker");
} // namespace

future<size_t> block_device_mocker_impl::write(uint64_t pos, const void* buffer, size_t len, const io_priority_class&) {
    mlogger.debug("write({}, ..., {})", pos, len);
    BOOST_REQUIRE_EQUAL(pos % alignment, 0);
    BOOST_REQUIRE_EQUAL(reinterpret_cast<uintptr_t>(buffer) % alignment, 0);
    BOOST_REQUIRE_EQUAL(len % alignment, 0);
    writes.emplace_back(write_operation{
        .disk_offset = pos,
        .data = temporary_buffer<uint8_t>(static_cast<const uint8_t*>(buffer), len),
    });
    if (buf.size() < pos + len) {
        buf.resize(pos + len);
    }
    std::memcpy(buf.data() + pos, buffer, len);
    return make_ready_future<size_t>(len);
}

future<size_t> block_device_mocker_impl::read(uint64_t pos, void* buffer, size_t len, const io_priority_class&) noexcept {
    mlogger.debug("read({}, ..., {})", pos, len);
    BOOST_REQUIRE_EQUAL(pos % alignment, 0);
    BOOST_REQUIRE_EQUAL(reinterpret_cast<uintptr_t>(buffer) % alignment, 0);
    BOOST_REQUIRE_EQUAL(len % alignment, 0);
    if (buf.size() < pos + len) {
        buf.resize(pos + len);
    }
    std::memcpy(buffer, buf.c_str() + pos, len);
    return make_ready_future<size_t>(len);
}

} // namespace seastar::fs
