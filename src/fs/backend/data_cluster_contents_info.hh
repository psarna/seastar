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

#include "fs/backend/inode_info.hh"
#include "seastar/fs/unit_types.hh"

#include <map>

namespace seastar::fs::backend {

class data_cluster_contents_info {
public:
    // Each cluster_data_vec corresponds to an inode_data_vec
    struct cluster_data_vec {
        inode_t data_owner;
        file_range data_range;
    };

private:
    size_t _up_to_date_data_size = 0;
    std::map<disk_offset_t, cluster_data_vec> _data;

public:
    const std::map<disk_offset_t, cluster_data_vec>& get_data() const noexcept {
        return _data;
    }

    bool is_empty() const noexcept {
        return _up_to_date_data_size == 0;
    }

    void add_data(disk_offset_t disk_offset, inode_t inode, file_range data_range) noexcept {
        auto [it, inserted] = _data.emplace(disk_offset, cluster_data_vec {inode, data_range});
        assert(inserted);
        _up_to_date_data_size += data_range.size();
        auto pv = std::prev(it);
        assert(it == _data.begin() || disk_offset >= pv->first + pv->second.data_range.size());
        auto nx = std::next(it);
        assert(nx == _data.end() || nx->first >= disk_offset + data_range.size());
    }

    void cut_data(disk_offset_t disk_offset, file_range former_range, file_range new_left_range, file_range new_right_range) {
        auto data_vec_nh = _data.extract(disk_offset);
        assert(!data_vec_nh.empty());
        assert(data_vec_nh.mapped().data_range == former_range);
        assert(former_range.beg == new_left_range.beg && former_range.end == new_right_range.end);
        assert(new_left_range.end <= new_right_range.beg);
        assert(new_left_range.size() >= 0 && new_right_range.size() >= 0);
        auto inode = data_vec_nh.mapped().data_owner;
        if (!new_left_range.is_empty()) {
            data_vec_nh.mapped() = {inode, new_left_range};
            _data.insert(std::move(data_vec_nh));
        }
        if (!new_right_range.is_empty()) {
            _data.emplace(disk_offset + former_range.size() - new_right_range.size(), cluster_data_vec {inode, new_right_range});
        }
        _up_to_date_data_size -= former_range.size() - new_left_range.size() - new_right_range.size();
    }
};

} // namespace seastar::fs::backend
