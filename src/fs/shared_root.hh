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

#include "seastar/core/future.hh"
#include "seastar/core/sharded.hh"
#include "seastar/core/shared_ptr.hh"

namespace seastar::fs {

/* TODO: maybe we want sorted map here */
using shared_entries = std::unordered_map<std::string, unsigned>;

class shared_root {
    shared_entries _root;
public:
    shared_root() = default;

    shared_root(const shared_root&) = delete;
    shared_root& operator=(const shared_root&) = delete;
    shared_root(shared_root&&) = default;

    void remove_entry(std::string path);

    shared_entries get_entries();

    /* TODO: use bool_class idiom */
    bool try_add_entry(std::string path, unsigned shard_id);

    void add_entries(shared_entries root_shard);
};

/* TODO: split logic on local_shared_root and foreign_shared_root */
class foreign_shared_root {
    foreign_ptr<lw_shared_ptr<shared_root>> _shared_root;
public:
    foreign_shared_root() = default;
    explicit foreign_shared_root(lw_shared_ptr<shared_root> root) : _shared_root(make_foreign(std::move(root))) {}

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

}
