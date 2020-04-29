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

#include "fs/shared_root.hh"

#include "seastar/core/smp.hh"
#include "seastar/fs/exceptions.hh"

namespace seastar::fs {

inline void throw_if_empty(const std::string& path) {
    if (__builtin_expect(path.empty(), false)) {
        throw invalid_path_exception();
    }
}

inline void throw_if_root(const std::string& path) {
    if (__builtin_expect(path == "/", false)) {
        throw cannot_modify_root_exception();
    }
}

bool shared_root::try_add_entry(std::string path, unsigned shard_id) {
    throw_if_empty(path);
    throw_if_root(path);

    const auto can_add = _root.find(path) == _root.end();

    if (can_add) {
        _root[path] = shard_id;
    }

    return can_add;
}

void shared_root::remove_entry(std::string path) {
    _root.erase(path);
}

shared_entries shared_root::get_entries() {
    return _root;
}

void shared_root::add_entries(shared_entries root_shard) {
    _root.insert(root_shard.begin(), root_shard.end());
}

future<shared_entries> foreign_shared_root::get_entries() {
    if (_shared_root.get_owner_shard() == this_shard_id()) {
        return make_ready_future<shared_entries>(_shared_root->get_entries());
    }

    return smp::submit_to(_shared_root.get_owner_shard(), [sr = _shared_root.get()]() mutable {
        return sr->get_entries();
    });
}

future<> foreign_shared_root::remove_entry(std::string path) {
    if (_shared_root.get_owner_shard() == this_shard_id()) {
        _shared_root->remove_entry(std::move(path));
        return make_ready_future();
    }

    return smp::submit_to(_shared_root.get_owner_shard(),
            [sr = _shared_root.get(), path = std::move(path)]() mutable {
        return sr->remove_entry(std::move(path));
    });
}

future<bool> foreign_shared_root::try_add_entry(std::string path) {
    auto shard_id = this_shard_id();
    if (_shared_root.get_owner_shard() == shard_id) {
        return make_ready_future<bool>(_shared_root->try_add_entry(std::move(path), shard_id));
    }

    return smp::submit_to(_shared_root.get_owner_shard(),
            [sr = _shared_root.get(), path = std::move(path), shard_id]() mutable {
        return sr->try_add_entry(std::move(path), shard_id);
    });
}

future<> foreign_shared_root::add_entries(shared_entries entries) {
    if (_shared_root.get_owner_shard() == this_shard_id()) {
        _shared_root->add_entries(std::move(entries));
        return make_ready_future();
    }

    return smp::submit_to(_shared_root.get_owner_shard(),
            [sr = _shared_root.get(), entries = std::move(entries)]() mutable {
        return sr->add_entries(std::move(entries));
    });
}

}