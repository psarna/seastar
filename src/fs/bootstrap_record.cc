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

#include "fs/bootstrap_record.hh"

#include <boost/crc.hpp>
#include <seastar/core/units.hh>

namespace seastar::fs {

namespace {

constexpr size_t alignment = 4 * KB;
constexpr offset_t boot_record_offset = 0;
constexpr unit_size_t min_sector_size = 512;

constexpr size_t aligned_boot_record_size =
        ((sizeof(bootstrap_record_disk) / alignment) + (sizeof(bootstrap_record_disk) % alignment > 0)) * alignment;
constexpr size_t crc_offset = offsetof(bootstrap_record_disk, crc);

uint32_t crc32(const char* buff, size_t len) {
    boost::crc_32_type result;
    result.process_bytes(buff, len);
    return result.checksum();
}

bool is_sector_size_valid(unit_size_t sector_size) {
    return is_power_of_2(sector_size) && sector_size >= min_sector_size;
}

bool is_cluster_size_valid(unit_size_t cluster_size, unit_size_t sector_size) {
    return is_power_of_2(cluster_size) && cluster_size % sector_size == 0;
}

future<size_t> read_exactly(block_device& device, offset_t off, char* buff, size_t size) {
    return do_with((size_t)0, [&device, off, buff, size] (auto& count) {
        return repeat([&count, &device, off, buff, size] () {
            return device.read(off + count, buff, size - count).then([&count] (size_t ret) {
                if (ret == 0 || ret % alignment)
                    return stop_iteration::yes;
                count += ret;
                return stop_iteration::no;
            });
        }).then([&count] {
            return count;
        });
    });
}

future<size_t> write_exactly(block_device& device, offset_t off, const char* buff, size_t size) {
    return do_with((size_t)0, [&device, off, buff, size] (auto& count) {
        return repeat([&count, &device, off, buff, size] () {
            return device.write(off + count, buff, size - count).then([&count] (size_t ret) {
                if (ret == 0 || ret % alignment)
                    return stop_iteration::yes;
                count += ret;
                return stop_iteration::no;
            });
        }).then([&count] {
            return count;
        });
    });
}

}

future<bootstrap_record> bootstrap_record::read_from_disk(block_device& device) {
    auto boot_record_buff = temporary_buffer<char>::aligned(alignment, aligned_boot_record_size);
    return read_exactly(device, boot_record_offset, boot_record_buff.get_write(), aligned_boot_record_size)
            .then([boot_record_buff = std::move(boot_record_buff)] (size_t ret) {
        if (ret != aligned_boot_record_size) {
            return make_exception_future<bootstrap_record>(
                    invalid_bootstrap_record("Error while reading bootstrap record block"));
        }

        bootstrap_record_disk boot_record_disk;
        std::memcpy(&boot_record_disk, boot_record_buff.get(), sizeof(boot_record_disk));

        const uint32_t crc_calc = crc32(boot_record_buff.get(), crc_offset);
        if (crc_calc != boot_record_disk.crc) {
            return make_exception_future<bootstrap_record>(invalid_bootstrap_record("Read CRC is invalid"));
        }
        if (magic_number != boot_record_disk.magic) {
            return make_exception_future<bootstrap_record>(invalid_bootstrap_record("Read Magic Number is invalid"));
        }
        if (boot_record_disk.shards_nb > max_shards_nb) {
            return make_exception_future<bootstrap_record>(
                    invalid_bootstrap_record("Read number of shards is bigger than max number of shards"));
        }
        if (!is_sector_size_valid(boot_record_disk.sector_size)) {
            return make_exception_future<bootstrap_record>(
                    invalid_bootstrap_record("Sector size should be a power of 2 and greater than 512"));
        }
        if (!is_cluster_size_valid(boot_record_disk.cluster_size, boot_record_disk.sector_size)) {
            return make_exception_future<bootstrap_record>(
                    invalid_bootstrap_record("Cluster size should be a power of 2 and be divisible by sector size"));
        }

        const std::vector<shard_info> shards_info(boot_record_disk.shards_info,
                boot_record_disk.shards_info + boot_record_disk.shards_nb);

        bootstrap_record boot_record_mem(boot_record_disk.version,
                boot_record_disk.sector_size,
                boot_record_disk.cluster_size,
                boot_record_disk.root_directory,
                boot_record_disk.shards_nb,
                std::move(shards_info));

        return make_ready_future<bootstrap_record>(boot_record_mem);
    });
}

future<> bootstrap_record::write_to_disk(block_device& device) const {
    // initial check
    if (shards_info.size() > max_shards_nb) {
        return make_exception_future<>(invalid_bootstrap_record("Too many shards to fit into the bootstrap record"));
    }
    else if (shards_nb != shards_info.size()) {
        return make_exception_future<>(
                invalid_bootstrap_record("Shards number should match number of metadata pointers"));
    }
    if (!is_sector_size_valid(sector_size)) {
        return make_exception_future<>(invalid_bootstrap_record("Sector size should be a power of 2 and greater than 512"));
    }
    if (!is_cluster_size_valid(cluster_size, sector_size)) {
        return make_exception_future<>(invalid_bootstrap_record("Cluster size should be a power of 2 and be divisible by sector size"));
    }

    bootstrap_record_disk boot_record_disk;
    std::memset(&boot_record_disk, 0, sizeof(boot_record_disk));

    // prepare bootstrap_record_disk records
    boot_record_disk.magic = bootstrap_record::magic_number;
    boot_record_disk.version = version;
    boot_record_disk.sector_size = sector_size;
    boot_record_disk.cluster_size = cluster_size;
    boot_record_disk.root_directory = root_directory;
    boot_record_disk.shards_nb = shards_nb;
    std::copy(shards_info.begin(), shards_info.end(), boot_record_disk.shards_info);

    auto boot_record_buff = temporary_buffer<char>::aligned(alignment, aligned_boot_record_size);
    std::memcpy(boot_record_buff.get_write(), &boot_record_disk, sizeof(boot_record_disk));
    std::memset(boot_record_buff.get_write() + sizeof(boot_record_disk), 0,
            aligned_boot_record_size - sizeof(boot_record_disk));

    boot_record_disk.crc = crc32(boot_record_buff.get(), crc_offset);
    std::memcpy(boot_record_buff.get_write() + crc_offset, &boot_record_disk.crc, sizeof(boot_record_disk.crc));

    return write_exactly(device, boot_record_offset, boot_record_buff.get(), aligned_boot_record_size)
            .then([boot_record_buff = std::move(boot_record_buff)] (size_t ret) {
        if (ret != aligned_boot_record_size) {
            return make_exception_future<>(
                    invalid_bootstrap_record("Error while writing bootstrap record block to disk"));
        }
        return make_ready_future<>();
    });
}

bool bootstrap_record::shard_info::operator==(const shard_info &rhs) const noexcept {
    return metadata_cl == rhs.metadata_cl && range_cl == rhs.range_cl;
}

bool bootstrap_record::operator==(const bootstrap_record &rhs) const noexcept {
    return version == rhs.version && sector_size == rhs.sector_size && cluster_size == rhs.cluster_size &&
            root_directory == rhs.root_directory &&  shards_info == rhs.shards_info;
}

}
