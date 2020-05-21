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

#include "seastar/core/smp.hh"
#include "seastar/fs/exceptions.hh"
#include "seastar/fs/shared_root.hh"

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

bool global_shared_root::try_add_entry(std::string path, unsigned shard_id) {
    throw_if_empty(path);
    throw_if_root(path);

    const auto can_add = _root.find(path) == _root.end();

    if (can_add) {
        _root[path] = shard_id;
    }

    return can_add;
}

void global_shared_root::remove_entry(std::string path) {
    _root.erase(path);
}

shared_entries global_shared_root::get_entries() {
    return _root;
}

void global_shared_root::add_entries(shared_entries root_shard) {
    _root.insert(root_shard.begin(), root_shard.end());
}

class local_shared_root final : public shared_root_impl {
    lw_shared_ptr<global_shared_root> _shared_root;
public:
    local_shared_root() = default;
    explicit local_shared_root(lw_shared_ptr<global_shared_root> root) : _shared_root(std::move(root)) {}

    local_shared_root(const local_shared_root&) = delete;
    local_shared_root& operator=(const local_shared_root&) = delete;
    local_shared_root(local_shared_root&&) = default;
    local_shared_root& operator=(local_shared_root&& other) = default;

    future<shared_entries> get_entries();

    future<> remove_entry(std::string path);

    /* TODO: use bool_class idiom */
    future<bool> try_add_entry(std::string path);

    future<> add_entries(shared_entries entries);
};

future<shared_entries> local_shared_root::get_entries() {
    return make_ready_future<shared_entries>(_shared_root->get_entries());
}

future<> local_shared_root::remove_entry(std::string path) {
    _shared_root->remove_entry(std::move(path));
    return make_ready_future();
}

future<bool> local_shared_root::try_add_entry(std::string path) {
    return make_ready_future<bool>(_shared_root->try_add_entry(std::move(path), this_shard_id()));
}

future<> local_shared_root::add_entries(shared_entries entries) {
    _shared_root->add_entries(std::move(entries));
    return make_ready_future();
}

class foreign_shared_root final : public shared_root_impl {
    foreign_ptr<lw_shared_ptr<global_shared_root>> _shared_root;
public:
    foreign_shared_root() = default;
    explicit foreign_shared_root(foreign_ptr<lw_shared_ptr<global_shared_root>> root) : _shared_root(std::move(root)) {}

    foreign_shared_root(const foreign_shared_root&) = delete;
    foreign_shared_root& operator=(const foreign_shared_root&) = delete;
    foreign_shared_root(foreign_shared_root&&) = default;
    foreign_shared_root& operator=(foreign_shared_root&& other) = default;

    future<shared_entries> get_entries();

    future<> remove_entry(std::string path);

    /* TODO: use bool_class idiom */
    future<bool> try_add_entry(std::string path);

    future<> add_entries(shared_entries entries);
};

future<shared_entries> foreign_shared_root::get_entries() {
    assert(_shared_root.get_owner_shard() != this_shard_id()); // TODO: exception

    return smp::submit_to(_shared_root.get_owner_shard(), [sr = _shared_root.get()]() mutable {
        return sr->get_entries();
    });
}

future<> foreign_shared_root::remove_entry(std::string path) {
    assert(_shared_root.get_owner_shard() != this_shard_id()); // TODO: exception

    return smp::submit_to(_shared_root.get_owner_shard(),
            [sr = _shared_root.get(), path = std::move(path)]() mutable {
        return sr->remove_entry(std::move(path));
    });
}

future<bool> foreign_shared_root::try_add_entry(std::string path) {
    assert(_shared_root.get_owner_shard() != this_shard_id()); // TODO: exception

    return smp::submit_to(_shared_root.get_owner_shard(),
            [sr = _shared_root.get(), path = std::move(path), shard_id = this_shard_id()]() mutable {
        return sr->try_add_entry(std::move(path), shard_id);
    });
}

future<> foreign_shared_root::add_entries(shared_entries entries) {
    assert(_shared_root.get_owner_shard() != this_shard_id()); // TODO: exception

    return smp::submit_to(_shared_root.get_owner_shard(),
            [sr = _shared_root.get(), entries = std::move(entries)]() mutable {
        return sr->add_entries(std::move(entries));
    });
}

shared_ptr<shared_root_impl> make_shared_root_impl(foreign_ptr<lw_shared_ptr<global_shared_root>> root) {
    if (root.get_owner_shard() == this_shard_id()) {
        return make_shared<local_shared_root>(root.release());
    }

    return make_shared<foreign_shared_root>(std::move(root));
}

shared_root::shared_root()
    : _fs(nullptr)
{}

shared_root::shared_root(foreign_ptr<lw_shared_ptr<global_shared_root>> root, sharded<filesystem>& fs)
    : _shared_root(make_shared_root_impl(std::move(root)))
    , _fs(&fs)
{}

future<shared_entries> shared_root::get_entries() {
    return _shared_root->get_entries();
}

future<> shared_root::remove_entry(std::string path) {
    return _shared_root->remove_entry(std::move(path)).then([this] {
        return update_cache();
    });
}

future<bool> shared_root::try_add_entry(std::string path) {
    return _shared_root->try_add_entry(std::move(path)).then([this] (bool result) {
        return update_cache().then([result] {
            return make_ready_future<bool>(result);
        });
    });
}

future<> shared_root::add_entries(shared_entries entries) {
    return _shared_root->add_entries(std::move(entries));
}

future<> shared_root::update_cache() {
    return _fs->invoke_on_all([](filesystem& fs) {
        //return fs.update_cache(); // FIXME: invalid use of incomplete type
    });
}

}
