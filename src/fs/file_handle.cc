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

#include "fs/backend/shard.hh"
#include "seastar/fs/file_handle.hh"

namespace seastar::fs {

class local_file_handle_impl : virtual public seastarfs_file_handle_impl {
    shared_ptr<backend::shard> _bshard;
public:
    explicit local_file_handle_impl(shared_ptr<backend::shard> bshard, inode_t inode)
        : seastarfs_file_handle_impl(inode)
        , _bshard(std::move(bshard))
    {}

    ~local_file_handle_impl() override = default;

    future<size_t> write(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return _bshard->write(_inode.value(), pos, buffer, len, pc);
    }

    future<size_t> read(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return _bshard->read(_inode.value(), pos, buffer, len, pc);
    }

    future<> flush() override {
        return _bshard->flush_log();
    }

    future<stat_data> stat() override {
        return make_ready_future<stat_data>(_bshard->stat(_inode.value()));
    }

    future<> truncate(uint64_t length) override {
        return _bshard->truncate(_inode.value(), length);
    }

    future<file_offset_t> size() override {
        return make_ready_future<file_offset_t>(_bshard->file_size(_inode.value()));
    }

    future<> close() override {
        return _bshard->close_file(_inode.value()).then([this] {
            _inode.reset();
        });
    }
};

class foreign_file_handle_impl : virtual public seastarfs_file_handle_impl {
    foreign_ptr<shared_ptr<backend::shard>> _bshard;
public:
    explicit foreign_file_handle_impl(foreign_ptr<shared_ptr<backend::shard>> bshard, inode_t inode)
        : seastarfs_file_handle_impl(inode)
        , _bshard(std::move(bshard))
    {}

    ~foreign_file_handle_impl() override = default;

    future<size_t> write(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        throw_if_not_foreign();

        return smp::submit_to(_bshard.get_owner_shard(), [this, pos, buffer, len, &pc]() {
            return _bshard->write(_inode.value(), pos, buffer, len, pc);
        });
    }

    future<size_t> read(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        throw_if_not_foreign();

        return smp::submit_to(_bshard.get_owner_shard(), [this, pos, buffer, len, &pc]() {
            return _bshard->read(_inode.value(), pos, buffer, len, pc);
        });
    }

    future<> flush() override {
        throw_if_not_foreign();

        return smp::submit_to(_bshard.get_owner_shard(), [this]() {
            return _bshard->flush_log();
        });
    }

    future<stat_data> stat() override {
        throw_if_not_foreign();

        return smp::submit_to(_bshard.get_owner_shard(), [this]() {
            return make_ready_future<stat_data>(_bshard->stat(_inode.value()));
        });
    }

    future<> truncate(uint64_t length) override {
        throw_if_not_foreign();

        return smp::submit_to(_bshard.get_owner_shard(), [this, length]() {
            return _bshard->truncate(_inode.value(), length);
        });
    }

    future<file_offset_t> size() override {
        throw_if_not_foreign();

        return smp::submit_to(_bshard.get_owner_shard(), [this]() mutable {
            return make_ready_future<file_offset_t>(_bshard->file_size(_inode.value()));
        });
    }

    future<> close() override {
        throw_if_not_foreign();

        return smp::submit_to(_bshard.get_owner_shard(), [this]() mutable {
            return _bshard->close_file(_inode.value());
        }).then([this] {
            _inode.reset();
        });
    }
private:
    void throw_if_not_foreign() {
        if (__builtin_expect(_bshard.get_owner_shard() == this_shard_id(), false)) {
            throw file_used_on_unintended_shard_exception();
        }
    }
};

shared_file_handle
make_file_handle_impl(foreign_ptr<shared_ptr<backend::shard>> bshard, inode_t inode) {
    if (bshard.get_owner_shard() == this_shard_id()) {
        return make_shared<local_file_handle_impl>(bshard.release(), inode);
    }

    return make_shared<foreign_file_handle_impl>(std::move(bshard), inode);
}

}
