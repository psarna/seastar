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

future<> filesystem::flush() {
    if (!_backend_shard) {
        return make_ready_future();
    }
    return _backend_shard->flush_log();
}

size_t filesystem::remaining_space() {
    if (!_backend_shard) {
        return 0; // TODO: or throw?
    }
    return _backend_shard->remaining_space();
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

cluster_range which_cluster_bucket(cluster_range available_clusters, uint32_t shards_nb, uint32_t shard_id) {
    const cluster_id_t clusters_nb = available_clusters.end - available_clusters.beg;

    if (available_clusters.end < available_clusters.beg) {
        throw invalid_cluster_range_exception();
    }

    if (clusters_nb < shards_nb) {
        throw too_little_available_clusters_exception();
    }

    const uint32_t lower_bucket_size = clusters_nb / shards_nb;
    const uint32_t with_upper_bucket = clusters_nb % shards_nb;

    const cluster_id_t beg = shard_id * lower_bucket_size + std::min(shard_id, with_upper_bucket);
    const cluster_id_t end = (shard_id + 1) * lower_bucket_size + std::min(shard_id + 1, with_upper_bucket);

    return { available_clusters.beg + beg, available_clusters.beg + end };
}

future<> distribute_clusters(cluster_range available_clusters, shard_info_vec& shards_info) {
    const auto all_shards = boost::irange<uint32_t>(0, shards_info.size());
    return parallel_for_each(all_shards, [available_clusters, &shards_info](uint32_t shard_id) {
        const cluster_range bucket = which_cluster_bucket(available_clusters, shards_info.size(), shard_id);
        shards_info[shard_id] = { bucket.beg, bucket };
        return make_ready_future();
    });
}

future<bootstrap_record> make_bootstrap_record(uint64_t version, disk_offset_t alignment, disk_offset_t cluster_size,
        inode_t root_directory, uint32_t shards_nb, disk_offset_t block_device_size) {
    constexpr cluster_id_t first_available_cluster = 1; /* TODO: we don't support copies of bootstrap_record yet */
    const cluster_id_t last_available_cluster = offset_to_cluster_id(block_device_size, cluster_size);
    const cluster_range available_clusters = { first_available_cluster, last_available_cluster };

    return do_with(shard_info_vec(shards_nb), [=](auto& shards_info) {
        return distribute_clusters(available_clusters, shards_info).then([=, &shards_info] {
            return bootstrap_record(version, alignment, cluster_size, root_directory, shards_info);
        });
    });
}

future<size_t> write_zero_cluster(block_device& device, disk_offset_t alignment, disk_offset_t cluster_size,
        disk_offset_t offset) {
    return async([&, alignment, cluster_size, offset] {
        auto buf = allocate_aligned_buffer<uint8_t>(cluster_size, alignment);
        for (size_t i = 0; i < cluster_size; ++i) {
            thread::maybe_yield();
            buf[i] = 0;
        }
        auto written = device.write(offset, buf.get(), cluster_size).get0();
        return written;
    });
}

future<> invalid_metadata_clusters(block_device& device, std::vector<bootstrap_record::shard_info> shards_info,
        disk_offset_t alignment, disk_offset_t cluster_size) {
    return parallel_for_each(std::move(shards_info), [&device, alignment, cluster_size](bootstrap_record::shard_info shard_info) {
        return write_zero_cluster(device, alignment, cluster_size, shard_info.metadata_cluster * cluster_size).then(
                [cluster_size] (size_t ret) {
            if (ret != cluster_size) {
                return make_exception_future<>(filesystem_has_not_been_invalidated_exception());
            }
            return make_ready_future<>();
        });
    });
}

future<> bootfs(sharded<filesystem>& fs, std::string device_path) {
    return async([&fs, device_path = std::move(device_path)]() mutable {
        assert(thread::running_in_thread());
        auto root = make_lw_shared<global_shared_root>();
        fs.start().get();

        /* Each shard should have own version of shared_root */
        parallel_for_each(smp::all_cpus(), [&fs, device_path = std::move(device_path), root] (shard_id id) mutable {
            return fs.invoke_on(id, [device_path, root = make_foreign(root)](filesystem& f) mutable {
                return f.start(std::move(device_path), std::move(root));
            });
        }).get();
    });
}

future<> mkfs(std::string device_path, uint64_t version, disk_offset_t cluster_size, disk_offset_t alignment,
        inode_t root_directory, uint32_t shards_nb) {
    return async([device_path = std::move(device_path), version, cluster_size, alignment, root_directory, shards_nb] {
        assert(thread::running_in_thread());
        auto device = open_block_device(device_path).get0();
        auto close_dev = defer([device] () mutable { device.close().get(); });
        size_t device_size = device.size().get0();
        auto record = make_bootstrap_record(version, alignment, cluster_size, root_directory, shards_nb, device_size).get0();
        invalid_metadata_clusters(device, record.shards_info, alignment, cluster_size).get();
        record.write_to_disk(device).get();
    });
}

} // namespace seastar::fs
