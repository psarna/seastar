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
 * Copyright (C) 2019 ScyllaDB
 */

#pragma once

#include "fs/inode.hh"
#include "fs/unix_metadata.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/fs/overloaded.hh"
#include "seastar/fs/units.hh"

#include <map>
#include <variant>

namespace seastar::fs {

struct inode_data_vec {
    file_range data_range; // data spans [beg, end) range of the file

    struct in_mem_data {
        temporary_buffer<uint8_t> data;
    };

    struct on_disk_data {
        file_offset_t device_offset;
    };

    struct hole_data { };

    std::variant<in_mem_data, on_disk_data, hole_data> data_location;

    // TODO: rename that function to something more suitable
    inode_data_vec share_copy() {
        inode_data_vec shared;
        shared.data_range = data_range;
        std::visit(overloaded {
            [&](inode_data_vec::in_mem_data& mem) {
                shared.data_location = inode_data_vec::in_mem_data {mem.data.share()};
            },
            [&](inode_data_vec::on_disk_data& disk_data) {
                shared.data_location = disk_data;
            },
            [&](inode_data_vec::hole_data&) {
                shared.data_location = inode_data_vec::hole_data {};
            },
        }, data_location);
        return shared;
    }
};

struct inode_info {
    uint32_t opened_files_count = 0; // Number of open files referencing inode
    uint32_t directories_containing_file = 0;
    unix_metadata metadata;

    struct directory {
        // TODO: directory entry cannot contain '/' character --> add checks for that
        std::map<std::string, inode_t, std::less<>> entries; // entry name => inode
    };

    struct file {
        std::map<file_offset_t, inode_data_vec> data; // file offset => data vector that begins there (data vectors
                                                      // do not overlap)

        file_offset_t size() const noexcept {
            return (data.empty() ? 0 : data.rbegin()->second.data_range.end);
        }

        // Deletes data vectors that are subset of @p range and cuts overlapping data vectors to make them
        // not overlap. Calls @p data_vec_cutting_callback on every (even partially) intersecting inode_data_vec
        // with two inode_data_vecs representing its beginning and ending sections that don't intersect with
        // @p range as second and third parameter (these two can be non-empty only for inode_data_vecs
        // lying on the edges of @p range)
        template<class Func>
        void cut_out_data_range(file_range range, Func&& data_vec_cutting_callback) {
            static_assert(std::is_invocable_v<Func, inode_data_vec, inode_data_vec, inode_data_vec>);
            // Cut all vectors intersecting with range
            auto it = data.lower_bound(range.beg);
            if (it != data.begin() and are_intersecting(range, prev(it)->second.data_range)) {
                --it;
            }

            while (it != data.end() and are_intersecting(range, it->second.data_range)) {
                auto data_vec = std::move(data.extract(it++).mapped());
                const auto cap = intersection(range, data_vec.data_range);

                // If our range overlaps with current data_vec, we cut out the intersection leaving at most two parts
                // |       data_vec      |
                // | left  | cap | right |
                // left and right remain unless empty
                inode_data_vec left, right;
                left.data_range = {data_vec.data_range.beg, cap.beg};
                right.data_range = {cap.end, data_vec.data_range.end};
                auto right_beg_shift = right.data_range.beg - data_vec.data_range.beg;
                std::visit(overloaded {
                    [&](inode_data_vec::in_mem_data& mem) {
                        left.data_location = inode_data_vec::in_mem_data {mem.data.share(0, left.data_range.size())};
                        right.data_location = inode_data_vec::in_mem_data {
                            mem.data.share(right_beg_shift, right.data_range.size())
                        };
                    },
                    [&](inode_data_vec::on_disk_data& disk_data) {
                        left.data_location = disk_data;
                        right.data_location = inode_data_vec::on_disk_data {disk_data.device_offset + right_beg_shift};
                    },
                    [&](inode_data_vec::hole_data&) {
                        left.data_location = inode_data_vec::hole_data {};
                        right.data_location = inode_data_vec::hole_data {};
                    },
                }, data_vec.data_location);

                data_vec_cutting_callback(data_vec, left, right);

                // Save new data vectors
                if (not left.data_range.is_empty()) {
                    data.emplace(left.data_range.beg, std::move(left));
                }
                if (not right.data_range.is_empty()) {
                    data.emplace(right.data_range.beg, std::move(right));
                }
            }
        }

        // Executes @p execute_on_data_ranges_processor on each data vector that is a subset of @p data_range.
        // Data vectors on the edges are appropriately trimmed before passed to the function.
        template<class Func>
        void execute_on_data_range(file_range range, Func&& execute_on_data_range_processor) {
            static_assert(std::is_invocable_v<Func, inode_data_vec>);
            auto it = data.lower_bound(range.beg);
            if (it != data.begin() and are_intersecting(range, prev(it)->second.data_range)) {
                --it;
            }

            while (it != data.end() and are_intersecting(range, it->second.data_range)) {
                auto& data_vec = (it++)->second;
                const auto cap = intersection(range, data_vec.data_range);
                if (cap == data_vec.data_range) {
                    // Fully intersects => execute
                    execute_on_data_range_processor(data_vec.share_copy());
                    continue;
                }

                inode_data_vec mid;
                mid.data_range = std::move(cap);
                auto mid_beg_shift = mid.data_range.beg - data_vec.data_range.beg;
                std::visit(overloaded {
                    [&](inode_data_vec::in_mem_data& mem) {
                        mid.data_location = inode_data_vec::in_mem_data {
                            mem.data.share(mid_beg_shift, mid.data_range.size())
                        };
                    },
                    [&](inode_data_vec::on_disk_data& disk_data) {
                        mid.data_location = inode_data_vec::on_disk_data {disk_data.device_offset + mid_beg_shift};
                    },
                    [&](inode_data_vec::hole_data&) {
                        mid.data_location = inode_data_vec::hole_data {};
                    },
                }, data_vec.data_location);

                // Execute on middle range
                execute_on_data_range_processor(std::move(mid));
            }
        }
    };

    std::variant<directory, file> contents;

    bool is_linked() const noexcept {
        return directories_containing_file != 0;
    }

    bool is_open() const noexcept {
        return opened_files_count != 0;
    }

    constexpr bool is_directory() const noexcept { return std::holds_alternative<directory>(contents); }

    // These are noexcept because invalid access is a bug not an error
    constexpr directory& get_directory() & noexcept { return std::get<directory>(contents); }
    constexpr const directory& get_directory()  const & noexcept { return std::get<directory>(contents); }
    constexpr directory&& get_directory() && noexcept { return std::move(std::get<directory>(contents)); }

    constexpr bool is_file() const noexcept { return std::holds_alternative<file>(contents); }

    // These are noexcept because invalid access is a bug not an error
    constexpr file& get_file() & noexcept { return std::get<file>(contents); }
    constexpr const file& get_file()  const & noexcept { return std::get<file>(contents); }
    constexpr file&& get_file() && noexcept { return std::move(std::get<file>(contents)); }
};

} // namespace seastar::fs
