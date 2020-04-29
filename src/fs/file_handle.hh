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

#include "fs/metadata_log.hh"
#include "fs/units.hh"

#include "seastar/core/file.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/sharded.hh"

namespace seastar::fs {

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

class local_file_handle_impl : virtual public seastarfs_file_handle_impl {
    lw_shared_ptr<metadata_log> _log;
public:
    explicit local_file_handle_impl(lw_shared_ptr<metadata_log> log, inode_t inode)
    : seastarfs_file_handle_impl(inode)
    , _log(std::move(log))
    {}

    ~local_file_handle_impl() override = default;

    future<size_t> write(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return _log->write(_inode.value(), pos, buffer, len, pc);
    }

    future<size_t> read(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return _log->read(_inode.value(), pos, buffer, len, pc);
    }

    future<> flush() override {
        return _log->flush_log();
    }

    future<stat_data> stat() override {
        return make_ready_future<stat_data>(_log->stat(_inode.value()));
    }

    future<> truncate(uint64_t length) override {
        return _log->truncate(_inode.value(), length);
    }

    future<file_offset_t> size() override {
        return make_ready_future<file_offset_t>(_log->file_size(_inode.value()));
    }

    future<> close() override {
        return _log->close_file(_inode.value()).then([this] {
            _inode.reset();
        });
    }
};

class foreign_file_handle_impl : virtual public seastarfs_file_handle_impl {
    foreign_ptr<lw_shared_ptr<metadata_log>> _log;
public:
    explicit foreign_file_handle_impl(lw_shared_ptr<metadata_log> log, inode_t inode)
    : seastarfs_file_handle_impl(inode)
    , _log(make_foreign(std::move(log)))
    {}

    ~foreign_file_handle_impl() override = default;

    future<size_t> write(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        throw_if_not_foreign();

        return smp::submit_to(_log.get_owner_shard(), [this, pos, buffer, len, &pc]() {
            return _log->write(_inode.value(), pos, buffer, len, pc);
        });
    }

    future<size_t> read(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        throw_if_not_foreign();

        return smp::submit_to(_log.get_owner_shard(), [this, pos, buffer, len, &pc]() {
            return _log->read(_inode.value(), pos, buffer, len, pc);
        });
    }

    future<> flush() override {
        throw_if_not_foreign();

        return smp::submit_to(_log.get_owner_shard(), [this]() {
            return _log->flush_log();
        });
    }

    future<stat_data> stat() override {
        throw_if_not_foreign();

        return smp::submit_to(_log.get_owner_shard(), [this]() {
            return make_ready_future<stat_data>(_log->stat(_inode.value()));
        });
    }

    future<> truncate(uint64_t length) override {
        throw_if_not_foreign();

        return smp::submit_to(_log.get_owner_shard(), [this, length]() {
            return _log->truncate(_inode.value(), length);
        });
    }

    future<file_offset_t> size() override {
        throw_if_not_foreign();

        return smp::submit_to(_log.get_owner_shard(), [this]() mutable {
            return make_ready_future<file_offset_t>(_log->file_size(_inode.value()));
        });
    }

    future<> close() override {
        throw_if_not_foreign();

        return smp::submit_to(_log.get_owner_shard(), [this]() mutable {
            return _log->close_file(_inode.value());
        }).then([this] {
            _inode.reset();
        });
    }
private:
    void throw_if_not_foreign() {
        if (__builtin_expect(_log.get_owner_shard() == this_shard_id(), false)) {
            throw file_used_on_unintended_shard_exception();
        }
    }
};

inline future<shared_file_handle>
make_seastarfs_file_handle_impl(lw_shared_ptr<metadata_log> log, inode_t inode, unsigned caller_id) {
    if (caller_id == this_shard_id()) {
        return make_ready_future<shared_file_handle>(make_shared<local_file_handle_impl>(std::move(log), inode));
    }

    return make_ready_future<shared_file_handle>(make_shared<foreign_file_handle_impl>(std::move(log), inode));
}

}
