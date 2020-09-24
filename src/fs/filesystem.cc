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
#include "fs/bootstrap_record.hh"
#include "fs/cluster_utils.hh"
#include "fs/path.hh"
#include "seastar/core/shared_mutex.hh"
#include "seastar/core/thread.hh"
#include "seastar/core/units.hh"
#include "seastar/fs/file.hh"
#include "seastar/fs/filesystem.hh"
#include "seastar/fs/unit_types.hh"
#include "seastar/util/defer.hh"

namespace seastar::fs {

using shard_info_vec = std::vector<bootstrap_record::shard_info>;

void throw_if_empty(const std::string& path) {
    if (__builtin_expect(path.empty(), false)) {
        throw invalid_path_exception();
    }
}

void throw_if_root(const std::string& path) {
    if (__builtin_expect(path == "/", false)) {
        throw cannot_modify_root_exception();
    }
}

void throw_if_not_absolute(const std::string& path) {
    if (__builtin_expect(path.front() != '/', false)) {
        throw path_is_not_absolute_exception();
    }
}

future<> filesystem::start(std::string device_path, foreign_ptr<lw_shared_ptr<global_shared_root>> root) {
    return async([this, device_path = std::move(device_path), root = std::move(root)]() mutable {
        assert(thread::running_in_thread());
        _foreign_root = shared_root(std::move(root), container());

        auto device = open_block_device(device_path).get0();
        auto record =  bootstrap_record::read_from_disk(device).get0();

        const auto shard_id = this_shard_id();

        if (shard_id > record.shards_nb() - 1) {
            seastar_logger.warn("unable to boot filesystem on shard {}; performance may suffer", shard_id);
            device.close().get(); // TODO: Some shards cannot be launched, so implement reshard the whole dataset
            return;
        }

        const auto shard_info = record.shards_info[shard_id];

        _backend_shard = make_shared<backend::shard>(std::move(device), record.cluster_size, record.alignment);
        _backend_shard->bootstrap(record.root_directory, shard_info.metadata_cluster,
                shard_info.available_clusters, record.shards_nb(), shard_id).get();

        shared_entries l_root = local_root().get0();
        _cache_root.insert(l_root.begin(), l_root.end());
        _foreign_root.add_entries(_cache_root).get();
    });
}

future<> filesystem::stop() {
    if (!_backend_shard) {
        return make_ready_future();
    }

    return _backend_shard->shutdown();
}

future<shared_entries> filesystem::local_root() {
    return do_with(shared_entries(), [this](shared_entries& root) {
        return _backend_shard->iterate_directory("/", [&root] (const std::string& path) -> future<stop_iteration> {
            root[path] = this_shard_id();
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }).then([&root] {
            return root;
        });
    });
}

future<shared_entries> filesystem::global_root() {
    return _foreign_root.get_entries();
}

/* TODO: more efficient way to update cache */
future<> filesystem::update_cache() {
    return global_root().then([this](shared_entries root) {
        _cache_root.clear();
        _cache_root.insert(root.begin(), root.end());
    });
}

future<> filesystem::create_directory(std::string path) {
    /* TODO: reduce copy-paste */
    throw_if_empty(path);
    throw_if_root(path);
    throw_if_not_absolute(path);

    if (!path::is_canonical(path)) {
        path = path::canonical(path);
    }

    /* TODO: reduce copy-paste */
    const auto entry = path::root_entry(path);
    const bool is_entry = path::is_root_entry(path);
    const bool in_cache = _cache_root.find(entry) != _cache_root.end();

    if (is_entry && !in_cache) {
        // TODO: with_exclusive
        /* TODO: add a read-write lock to synchronize operations on the current shard. */
        return _foreign_root.try_add_entry(entry).then(
                [this, entry = std::move(entry), path = std::move(path)](bool result) {
            if (!result) {
                return make_ready_future(); //throw std::runtime_error("cannot add entry");
            }
            return _backend_shard->create_directory(std::move(path), file_permissions::default_file_permissions).handle_exception(
                    [this, path = std::move(path), entry = std::move(entry)] (auto exception) mutable {
                _cache_root.erase(entry);
                return _foreign_root.remove_entry(std::move(path)).then([e = std::move(exception)] {
                    return make_exception_future(e);
                });
            });
        });
    } else if (!is_entry && in_cache) {
        const auto entry_owner_id = _cache_root[entry];
        if (entry_owner_id == this_shard_id()) {
            return _backend_shard->create_directory(std::move(path), file_permissions::default_file_permissions);
        }

        /* TODO change to lambda */
        return container().invoke_on(entry_owner_id, &filesystem::create_directory, std::move(path));
    }

    return make_ready_future<>();//throw std::runtime_error("cannot add entry, try later");
}

future<file> filesystem::create_and_open_file(std::string name, open_flags flags) {
    return create_and_open_file_handler(std::move(name)).then(
            [flags](stub_file_handle fh) {
        return make_file(std::move(fh.bshard), fh.inode, flags);
    });
}

future<stub_file_handle> filesystem::create_and_open_inode(std::string path) {
    return _backend_shard->create_and_open_file(std::move(path), file_permissions::default_file_permissions).then(
            [this](inode_t inode) {
        stub_file_handle fh = {
            .bshard = make_foreign<shared_ptr<backend::shard>>(_backend_shard),
            .inode = inode,
        };
        return make_ready_future<stub_file_handle>(std::move(fh));
    });
}

future<stub_file_handle> filesystem::create_and_open_file_handler(std::string path) {
    throw_if_empty(path);
    throw_if_root(path);
    throw_if_not_absolute(path);

    if (!path::is_canonical(path)) {
        path = path::canonical(path);
    }

    /* TODO: reduce copy-paste */
    auto entry = path::root_entry(path);
    const bool is_entry = path::is_root_entry(path);
    const bool in_cache = _cache_root.find(entry) != _cache_root.end();

    if (is_entry && !in_cache) {
        // TODO: with_exclusive
        /* TODO: add a read-write lock to synchronize operations on the current shard. */
        return _foreign_root.try_add_entry(entry).then(
                [this, entry = std::move(entry), path = std::move(path)](bool result) mutable {
            if (!result) {
                throw std::runtime_error("cannot add entry");
            }
            return create_and_open_inode(path).handle_exception(
                [this, path = std::move(path), entry = std::move(entry)](auto exception) mutable {
                _cache_root.erase(entry);
                return _foreign_root.remove_entry(std::move(path)).then([e = std::move(exception)] {
                    return make_exception_future<stub_file_handle>(e);
                });
            });
        });
    } else if (!is_entry && in_cache) {
        const auto entry_owner_id = _cache_root[entry];
        if (entry_owner_id == this_shard_id()) {
            return create_and_open_inode(std::move(path));
        }

        /* TODO change to lambda */
        return container().invoke_on(entry_owner_id, &filesystem::create_and_open_file_handler, std::move(path));
    }

    throw std::runtime_error("cannot add entry, try later");
}

} // namespace seastar::fs
