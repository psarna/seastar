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

#include "fs/backend/metadata_log/entries.hh"
#include "fs/backend/metadata_log/to_disk_buffer.hh"
#include "fs_backend_metadata_log_entries_compare.hh"
#include "fs_backend_metadata_log_entries_print.hh"
#include "fs_variant.hh"
#include "seastar/fs/overloaded.hh"

#include <ostream>
#include <type_traits>
#include <variant>

namespace seastar::fs::backend::metadata_log {

struct mock_flush_entry {};
using mock_entry = extend_variant<entries::any_entry, mock_flush_entry>;

inline bool operator==(const mock_flush_entry&, const mock_flush_entry&) noexcept {
    return true;
}

template<class T, std::enable_if_t<has_alternative<T, mock_entry>, int> = 0>
inline bool operator==(const mock_entry& mentry, const T& entry) {
    if (!std::holds_alternative<T>(mentry)) {
        return false;
    }
    return std::get<T>(mentry) == entry;
}

inline std::ostream& operator<<(std::ostream& os, const mock_entry& mentry) {
    std::visit(overloaded{
        [&os](const entries::checkpoint& entry) { os << "[checkpoint=" << entry << ']'; },
        [&os](const entries::next_metadata_cluster& entry) { os << "[next_metadata_cluster=" << entry << ']'; },
        [&os](const entries::create_inode& entry) { os << "[create_inode=" << entry << ']'; },
        [&os](const entries::delete_inode& entry) { os << "[delete_inode=" << entry << ']'; },
        [&os](const entries::small_write& entry) { os << "[small_write=" << entry << ']'; },
        [&os](const entries::medium_write& entry) { os << "[medium_write=" << entry << ']'; },
        [&os](const entries::large_write& entry) { os << "[large_write=" << entry << ']'; },
        [&os](const entries::large_write_without_time& entry) { os << "[large_write_without_time=" << entry << ']'; },
        [&os](const entries::truncate& entry) { os << "[truncate=" << entry << ']'; },
        [&os](const entries::create_dentry& entry) { os << "[create_dentry=" << entry << ']'; },
        [&os](const entries::create_inode_as_dentry& entry) { os << "[create_inode_as_dentry=" << entry << ']'; },
        [&os](const entries::delete_dentry& entry) { os << "[delete_dentry=" << entry << ']'; },
        [&os](const entries::delete_inode_and_dentry& entry) { os << "[delete_inode_and_dentry=" << entry << ']'; },
        [&os](const mock_flush_entry&) { os << "[flush]"; },
    }, mentry);
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const std::vector<mock_entry>& mentries) {
    for (auto& mentry : mentries) {
        os << mentry << '\n';
    }
    return os;
}

class to_disk_buffer_mocker : public to_disk_buffer {
public:
    // A container with all the buffers created by virtual_constructor
    std::vector<shared_ptr<to_disk_buffer_mocker>>* all_buffers;

    std::vector<mock_entry> actions;

    explicit to_disk_buffer_mocker(decltype(all_buffers) all_buffers) : all_buffers(all_buffers) {}

    shared_ptr<to_disk_buffer> virtual_constructor() const override {
        auto new_buffer = seastar::make_shared<to_disk_buffer_mocker>(all_buffers);
        if (all_buffers) {
            all_buffers->emplace_back(new_buffer);
        }
        return new_buffer;
    }

    std::optional<fs::to_disk_buffer::init_error> init(size_t aligned_max_size, disk_offset_t alignment,
            disk_offset_t cluster_beg_offset) override {
        actions.clear();
        return to_disk_buffer::init(aligned_max_size, alignment, cluster_beg_offset);
    }

    using to_disk_buffer::init_from_bootstrapped_cluster;

    future<> flush_to_disk(block_device device) override {
        actions.emplace_back(mock_flush_entry{});
        return to_disk_buffer::flush_to_disk(device);
    }

protected:
    using to_disk_buffer::start_new_unflushed_data;
    using to_disk_buffer::prepare_unflushed_data_for_flush;
    using to_disk_buffer::acknowledge_write;

public:
    using to_disk_buffer::bytes_left;
    using to_disk_buffer::bytes_left_after_flush_if_done_now;

protected:
    template<class T>
    append_result append_impl(T&& entry) noexcept {
        switch (to_disk_buffer::append(entry)) {
        case APPENDED:
            // If emplace_back fails, we will fail nonetheless (as we are unit testing) and noexcept is required by
            // to_disk_buffer::append
            actions.emplace_back(std::forward<T>(entry));
            return APPENDED;
        case TOO_BIG: return TOO_BIG;
        }
        assert(false);
    }

public:
    append_result append(const entries::next_metadata_cluster& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::create_inode& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::delete_inode& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::small_write& entry) noexcept override {
        return append_impl(entries::small_write{
            .inode = entry.inode,
            .offset = entry.offset,
            .time_ns = entry.time_ns,
            .data = entry.data.clone()
        });
    }
    append_result append(const entries::medium_write& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::large_write& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::large_write_without_time& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::truncate& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::create_dentry& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::create_inode_as_dentry& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::delete_dentry& entry) noexcept override { return append_impl(entry); }
    append_result append(const entries::delete_inode_and_dentry& entry) noexcept override { return append_impl(entry); }
};

} // namespace seastar::fs
