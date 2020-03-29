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

#include "fs/metadata_to_disk_buffer.hh"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/fs/overloaded.hh>

#include <cstdint>
#include <variant>
#include <vector>

namespace seastar::fs {

struct ondisk_small_write {
    ondisk_small_write_header header;
    temporary_buffer<uint8_t> data;
};

struct ondisk_add_dir_entry {
    ondisk_add_dir_entry_header header;
    temporary_buffer<uint8_t> entry_name;
};

struct ondisk_create_inode_as_dir_entry {
    ondisk_create_inode_as_dir_entry_header header;
    temporary_buffer<uint8_t> entry_name;
};

struct ondisk_delete_dir_entry {
    ondisk_delete_dir_entry_header header;
    temporary_buffer<uint8_t> entry_name;
};

struct ondisk_delete_inode_and_dir_entry {
    ondisk_delete_inode_and_dir_entry_header header;
    temporary_buffer<uint8_t> entry_name;
};

class mock_metadata_to_disk_buffer : public metadata_to_disk_buffer {
public:
    mock_metadata_to_disk_buffer() = default;

    // A container with all the buffers created by virtual_constructor
    inline static thread_local std::vector<shared_ptr<mock_metadata_to_disk_buffer>> virtually_constructed_buffers;

    shared_ptr<metadata_to_disk_buffer> virtual_constructor() const override {
        auto new_buffer = make_shared<mock_metadata_to_disk_buffer>();
        virtually_constructed_buffers.emplace_back(new_buffer);
        return new_buffer;
    }

    struct action {
        struct append {
            using entry_data = std::variant<
                    ondisk_next_metadata_cluster,
                    ondisk_create_inode,
                    ondisk_delete_inode,
                    ondisk_small_write,
                    ondisk_medium_write,
                    ondisk_large_write,
                    ondisk_large_write_without_mtime,
                    ondisk_truncate,
                    ondisk_add_dir_entry,
                    ondisk_create_inode_as_dir_entry,
                    ondisk_delete_dir_entry,
                    ondisk_delete_inode_and_dir_entry>;

            entry_data entry;
        };

        struct flush_to_disk {};

        using action_data = std::variant<append, flush_to_disk>;

        action_data data;

        action(action_data data) : data(std::move(data)) {}
    };
    std::vector<action> actions;

    template<typename T>
    static bool is_type(const action& a) {
        return std::holds_alternative<T>(a.data);
    }

    template<typename T>
    static const T& get_by_type(const action& a) {
        return std::get<T>(a.data);
    }

    template<typename T>
    static bool is_append_type(const action& a) {
        return is_type<action::append>(a) and std::holds_alternative<T>(get_by_type<action::append>(a).entry);
    }

    template<typename T>
    static const T& get_by_append_type(const action& a) {
        return std::get<T>(get_by_type<action::append>(a).entry);
    }

    void init(size_t aligned_max_size, unit_size_t alignment, disk_offset_t cluster_beg_offset) override {
        assert(is_power_of_2(alignment));
        assert(mod_by_power_of_2(aligned_max_size, alignment) == 0);
        assert(mod_by_power_of_2(cluster_beg_offset, alignment) == 0);
        _max_size = aligned_max_size;
        _alignment = alignment;
        _cluster_beg_offset = cluster_beg_offset;
        _unflushed_data = {0, 0};
        start_new_unflushed_data();
    }

    void init_from_bootstrapped_cluster(size_t aligned_max_size, unit_size_t alignment,
            disk_offset_t cluster_beg_offset, size_t metadata_end_pos) noexcept override {
        assert(is_power_of_2(alignment));
        assert(mod_by_power_of_2(aligned_max_size, alignment) == 0);
        assert(mod_by_power_of_2(cluster_beg_offset, alignment) == 0);

        assert(aligned_max_size >= sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
        assert(alignment >= sizeof(ondisk_type) + sizeof(ondisk_checkpoint) + sizeof(ondisk_type) +
                sizeof(ondisk_next_metadata_cluster) and
                "We always need to be able to pack at least a checkpoint and next_metadata_cluster entry to the last "
                "data flush in the cluster");
        assert(metadata_end_pos < aligned_max_size);

        _max_size = aligned_max_size;
        _alignment = alignment;
        _cluster_beg_offset = cluster_beg_offset;
        auto aligned_pos = round_up_to_multiple_of_power_of_2(metadata_end_pos, _alignment);
        _unflushed_data = {aligned_pos, aligned_pos};

        start_new_unflushed_data();
    }

    future<> flush_to_disk([[maybe_unused]] block_device device) override {
        actions.emplace_back(action::flush_to_disk {});
        prepare_unflushed_data_for_flush();

        assert(mod_by_power_of_2(_unflushed_data.beg, _alignment) == 0);
        range real_write = {
            _unflushed_data.beg,
            round_up_to_multiple_of_power_of_2(_unflushed_data.end, _alignment),
        };

        // Make sure the buffer is usable before returning from this function
        _unflushed_data = {real_write.end, real_write.end};
        start_new_unflushed_data();

        return now();
    }

private:
    void mock_append_bytes(size_t len) {
        assert(len <= bytes_left());
        _unflushed_data.end += len;
    }

    append_result mock_append(size_t data_len) {
        if (not fits_for_append(data_len)) {
            return TOO_BIG;
        }
        mock_append_bytes(data_len);
        return APPENDED;
    }

    void start_new_unflushed_data() noexcept override {
        if (bytes_left() < sizeof(ondisk_type) + sizeof(ondisk_checkpoint) + sizeof(ondisk_type) +
                sizeof(ondisk_next_metadata_cluster)) {
            assert(bytes_left() == 0); // alignment has to be big enough to hold checkpoint and next_metadata_cluster
            return; // No more space
        }
        mock_append_bytes(sizeof(ondisk_type) + sizeof(ondisk_checkpoint));
    }

    void prepare_unflushed_data_for_flush() noexcept override {}

public:
    using metadata_to_disk_buffer::bytes_left_after_flush_if_done_now;
    using metadata_to_disk_buffer::bytes_left;
    using metadata_to_disk_buffer::append_result;

    append_result append(const ondisk_next_metadata_cluster& next_metadata_cluster) noexcept override {
        size_t len = ondisk_entry_size(next_metadata_cluster);
        if (bytes_left() < len) {
            return TOO_BIG;
        }
        actions.emplace_back(action::append {ondisk_next_metadata_cluster {next_metadata_cluster}});
        mock_append_bytes(len);
        return APPENDED;
    }

    append_result append(const ondisk_create_inode& create_inode) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(create_inode));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_create_inode {create_inode}});
        }
        return ret;
    }

    append_result append(const ondisk_delete_inode& delete_inode) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(delete_inode));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_delete_inode {delete_inode}});
        }
        return ret;
    }

    append_result append(const ondisk_medium_write& medium_write) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(medium_write));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_medium_write {medium_write}});
        }
        return ret;
    }

    append_result append(const ondisk_large_write& large_write) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(large_write));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_large_write {large_write}});
        }
        return ret;
    }

    append_result append(const ondisk_large_write_without_mtime& large_write_without_mtime) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(large_write_without_mtime));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_large_write_without_mtime {large_write_without_mtime}});
        }
        return ret;
    }

    append_result append(const ondisk_truncate& truncate) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(truncate));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_truncate {truncate}});
        }
        return ret;
    }

private:
    static temporary_buffer<uint8_t> copy_data(const void* data, size_t length) {
        return temporary_buffer<uint8_t>(static_cast<const uint8_t*>(data), length);
    }

public:
    append_result append(const ondisk_small_write_header& small_write, const void* data) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(small_write));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_small_write {
                    small_write,
                    copy_data(data, small_write.length)
                }});
        }
        return ret;
    }

    append_result append(const ondisk_add_dir_entry_header& add_dir_entry, const void* entry_name) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(add_dir_entry));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_add_dir_entry {
                    add_dir_entry,
                    copy_data(entry_name, add_dir_entry.entry_name_length)
                }});
        }
        return ret;
    }

    append_result append(const ondisk_create_inode_as_dir_entry_header& create_inode_as_dir_entry,
            const void* entry_name) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(create_inode_as_dir_entry));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_create_inode_as_dir_entry {
                    create_inode_as_dir_entry,
                    copy_data(entry_name, create_inode_as_dir_entry.entry_name_length)
                }});
        }
        return ret;
    }

    append_result append(const ondisk_delete_dir_entry_header& delete_dir_entry, const void* entry_name) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(delete_dir_entry));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_delete_dir_entry {
                    delete_dir_entry,
                    copy_data(entry_name, delete_dir_entry.entry_name_length)
                }});
        }
        return ret;
    }

    append_result append(const ondisk_delete_inode_and_dir_entry_header& delete_inode_and_dir_entry, const void* entry_name) noexcept override {
        append_result ret = mock_append(ondisk_entry_size(delete_inode_and_dir_entry));
        if (ret == APPENDED) {
            actions.emplace_back(action::append {ondisk_delete_inode_and_dir_entry {
                    delete_inode_and_dir_entry,
                    copy_data(entry_name, delete_inode_and_dir_entry.entry_name_length)
                }});
        }
        return ret;
    }
};

} // namespace seastar::fs
