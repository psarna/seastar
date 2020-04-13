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

#include "fs/device_reader.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/shared_ptr.hh"

#include <cassert>
#include <cstring>

namespace seastar::fs {

size_t device_reader::copy_intersecting_data(device_reader::disk_data_raw dest_data, const disk_data& source_data) const {
    disk_range intersect = intersection(dest_data._disk_range, source_data._disk_range);

    if (intersect.is_empty()) {
        return 0;
    }

    disk_offset_t common_data_len = intersect.size();
    if (dest_data._disk_range.beg >= source_data._disk_range.beg) {
        // Source data starts before dest data
        disk_offset_t source_data_offset = dest_data._disk_range.beg - source_data._disk_range.beg;
        // TODO: maybe we should split that memcpy to multiple parts because large reads can lead
        // to spikes in latency
        std::memcpy(dest_data._data, source_data._data.get() + source_data_offset, common_data_len);
    } else {
        // Source data starts after dest data
        // TODO: not tested yet
        disk_offset_t dest_data_offset = source_data._disk_range.beg - dest_data._disk_range.beg;
        std::memcpy(dest_data._data + dest_data_offset, source_data._data.get(), common_data_len);
    }
    return common_data_len;
}

device_reader::disk_data device_reader::init_aligned_disk_data(disk_range range) const {
    device_reader::disk_data disk_read;
    disk_read._disk_range = {
        round_down_to_multiple_of_power_of_2(range.beg, _alignment),
        round_up_to_multiple_of_power_of_2(range.end, _alignment)
    };
    disk_read._data = temporary_buffer<uint8_t>::aligned(_alignment, disk_read._disk_range.size());
    return disk_read;
}

future<size_t> device_reader::read_to_cache_and_copy_intersection(disk_range range, device_reader::disk_data_raw dest_data) {
    _cached_disk_read = init_aligned_disk_data(range);
    return _device.read(_cached_disk_read._disk_range.beg, _cached_disk_read._data.get_write(),
            _cached_disk_read._disk_range.size(), _pc).then(
            [this, dest_data](size_t read_len) {
        _cached_disk_read._disk_range.end = _cached_disk_read._disk_range.beg + read_len;
        auto copied_data = copy_intersecting_data(dest_data, _cached_disk_read);
        return copied_data;
    });
}


future<size_t> device_reader::read(uint8_t* buffer, disk_offset_t disk_offset, size_t read_len) {
    device_reader::disk_data_raw disk_data_to_read = {{disk_offset, disk_offset + read_len}, buffer};
    if (size_t cache_read_len = 0;
            not _cached_disk_read._data.empty() and
            (cache_read_len = copy_intersecting_data(disk_data_to_read, _cached_disk_read)) > 0) {
        if (cache_read_len == read_len) {
            return make_ready_future<size_t>(read_len);
        }

        if (_cached_disk_read._disk_range.beg <= disk_data_to_read._disk_range.beg) {
            // Left sided intersection
            disk_data_to_read.trim_front(cache_read_len);
            return do_read(disk_data_to_read).then([cache_read_len](size_t read_len) {
                return cache_read_len + read_len;
            });
        }
        else if (_cached_disk_read._disk_range.end >= disk_data_to_read._disk_range.end) {
            // Left sided intersection
            // TODO: not tested yet
            disk_data_to_read.trim_back(cache_read_len);
            return do_read(disk_data_to_read).then(
                    [expected_read_size = disk_data_to_read._disk_range.size(),
                    cache_read_len, disk_data_to_read](size_t read_len) {
                return read_len + (read_len == disk_data_to_read._disk_range.size() ? 0 : cache_read_len);
            });
        }
        else {
            // Middle part intersection
            // TODO: not tested yet
            auto left_disk_data = disk_data_to_read;
            disk_data_to_read.trim_back(disk_data_to_read._disk_range.end - _cached_disk_read._disk_range.beg);
            auto right_disk_data = disk_data_to_read;
            disk_data_to_read.trim_front(_cached_disk_read._disk_range.end - disk_data_to_read._disk_range.beg);

            auto current_read_len = make_lw_shared<size_t>(0);
            return do_read(left_disk_data).then(
                    [expected_read_size = left_disk_data._disk_range.size(),
                    cache_read_len, current_read_len](size_t read_len) {
                *current_read_len += read_len;
                if (read_len != expected_read_size) {
                    return make_exception_future<>(partial_read_exception());
                }
                *current_read_len += cache_read_len;
                return now();
            }).then([this, right_disk_data, current_read_len] {
                return do_read(right_disk_data).then(
                        [expected_read_size = right_disk_data._disk_range.size(),
                        current_read_len](size_t read_len) {
                    *current_read_len += read_len;
                    if (read_len != expected_read_size) {
                        return make_exception_future<size_t>(partial_read_exception());
                    }
                    return make_ready_future<size_t>(*current_read_len);
                });
            }).handle_exception_type([current_read_len] (partial_read_exception&) {
                return *current_read_len;
            });
        }
    } else {
        return do_read(disk_data_to_read);
    }
}

future<size_t> device_reader::do_read(device_reader::disk_data_raw disk_data_to_read) {
    auto remaining_disk_data = make_lw_shared<device_reader::disk_data_raw>(disk_data_to_read);
    auto current_read_len = make_lw_shared<size_t>(0);

    if (remaining_disk_data->data_alignment(_alignment) == remaining_disk_data->range_alignment(_alignment)) {
        disk_range read_range {
            round_down_to_multiple_of_power_of_2(remaining_disk_data->_disk_range.beg, _alignment),
            round_up_to_multiple_of_power_of_2(remaining_disk_data->_disk_range.beg, _alignment)
        };
        // Read prefix
        return read_to_cache_and_copy_intersection(read_range, *remaining_disk_data).then(
                [this, remaining_disk_data, current_read_len, expected_read_len = read_range.size()](size_t copied_len) {
            remaining_disk_data->trim_front(copied_len);
            *current_read_len += copied_len;
            if (_cached_disk_read._disk_range.size() != expected_read_len) {
                return make_exception_future<>(partial_read_exception());
            }
            return now();
        }).then([this, remaining_disk_data, current_read_len] {
            // Read middle part (directly into the output buffer)
            if (remaining_disk_data->_disk_range.is_empty()) {
                return now();
            } else {
                size_t expected_read_len =
                        round_down_to_multiple_of_power_of_2(remaining_disk_data->_disk_range.size(), _alignment);
                assert(remaining_disk_data->_disk_range.beg % _alignment == 0);
                return _device.read(remaining_disk_data->_disk_range.beg, remaining_disk_data->_data, expected_read_len, _pc).then(
                        [remaining_disk_data, current_read_len, expected_read_len](size_t read_len) {

                    remaining_disk_data->trim_front(read_len);
                    *current_read_len += read_len;

                    if (read_len != expected_read_len) {
                        // Partial read - end here
                        return make_exception_future<>(partial_read_exception());
                    }

                    return now();
                });
            }
        }).then([this, remaining_disk_data, current_read_len] {
            if (remaining_disk_data->_disk_range.is_empty()) {
                return make_ready_future<size_t>(*current_read_len);
            } else {
                return read_to_cache_and_copy_intersection(remaining_disk_data->_disk_range, *remaining_disk_data).then(
                        [current_read_len](size_t copied_len) {
                    return *current_read_len + copied_len;
                });
            }
        }).handle_exception_type([current_read_len] (partial_read_exception&) {
            return *current_read_len;
        });
    } else {
        return read_to_cache_and_copy_intersection(remaining_disk_data->_disk_range, *remaining_disk_data).then(
                [current_read_len](size_t copied_len) {
            return *current_read_len + copied_len;
        });
    }
}

} // namespace seastar::fs