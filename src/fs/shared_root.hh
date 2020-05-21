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

#include "seastar/core/future.hh"
#include "seastar/core/sharded.hh"
#include "seastar/core/shared_ptr.hh"

namespace seastar::fs {

class filesystem;

/* TODO: maybe we want sorted map here */
using shared_entries = std::unordered_map<std::string, unsigned>;

class global_shared_root {
    shared_entries _root;
public:
    global_shared_root() = default;

    global_shared_root(const global_shared_root&) = delete;
    global_shared_root& operator=(const global_shared_root&) = delete;
    global_shared_root(global_shared_root&&) = default;

    void remove_entry(std::string path);

    shared_entries get_entries();

    /* TODO: use bool_class idiom or smth better (messages) */
    bool try_add_entry(std::string path, unsigned shard_id);

    void add_entries(shared_entries root_shard);
};

class shared_root_impl {
public:
    virtual ~shared_root_impl() = default;
    virtual future<shared_entries> get_entries() = 0;
    virtual future<> remove_entry(std::string path) = 0;
    /* TODO: use bool_class idiom or smth better (messages) */
    virtual future<bool> try_add_entry(std::string path) = 0;
    virtual future<> add_entries(shared_entries entries) = 0;
};

shared_ptr<shared_root_impl> make_shared_root_impl(foreign_ptr<lw_shared_ptr<global_shared_root>> root);

class shared_root {
    shared_ptr<shared_root_impl> _shared_root;
    sharded<fs::filesystem>* _fs; /* only for updating cache on all shards */
public:
    shared_root();
    explicit shared_root(foreign_ptr<lw_shared_ptr<global_shared_root>> root, sharded<filesystem>& fs);

    shared_root(const shared_root&) = delete;
    shared_root& operator=(const shared_root&) = delete;
    shared_root(shared_root&&) = default;
    shared_root& operator=(shared_root&& other) = default;

    future<shared_entries> get_entries();

    future<> remove_entry(std::string path);

    /* TODO: use bool_class idiom or smth better (messages) */
    future<bool> try_add_entry(std::string path);

    future<> add_entries(shared_entries entries);

    future<> update_cache();
};

} // namespace seastar::fs
