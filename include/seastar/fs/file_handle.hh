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
#include "seastar/core/reactor.hh"
#include "seastar/core/sharded.hh"
#include "seastar/fs/stat.hh"
#include "seastar/fs/units.hh"

namespace seastar::fs {

class metadata_log;
class seastarfs_file_handle_impl;

using shared_file_handle = shared_ptr<seastarfs_file_handle_impl>;

class seastarfs_file_handle_impl {
protected:
    std::optional<inode_t> _inode;
public:
    explicit seastarfs_file_handle_impl(inode_t inode) : _inode(inode) {}
    virtual ~seastarfs_file_handle_impl() { if (_inode) { seastar_logger.warn("inode has not been closed"); } }

    explicit operator bool() const noexcept { return bool(_inode); }

    virtual future<size_t> write(uint64_t pos, const void* buffer, size_t len, const io_priority_class &pc) = 0;
    virtual future<size_t> read(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) = 0;
    virtual future<> flush() = 0;
    virtual future<stat_data> stat() = 0;
    virtual future<> truncate(uint64_t length) = 0;
    virtual future<file_offset_t> size() = 0;
    virtual future<> close() = 0;
};

struct stub_file_handle {
    foreign_ptr<shared_ptr<metadata_log>> log;
    inode_t inode;
};

shared_file_handle make_file_handle_impl(foreign_ptr<shared_ptr<metadata_log>> log, inode_t inode);

}
