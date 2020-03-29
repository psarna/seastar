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

#include "fs/cluster.hh"
#include "fs/inode.hh"
#include "fs/unix_metadata.hh"

namespace seastar::fs {

enum ondisk_type : uint8_t {
    INVALID = 0,
    CHECKPOINT,
    NEXT_METADATA_CLUSTER,
};

struct ondisk_checkpoint {
    // The disk format is as follows:
    // | ondisk_checkpoint | .............................. |
    //                     |             data               |
    //                     |<-- checkpointed_data_length -->|
    //                                                      ^
    //       ______________________________________________/
    //      /
    //    there ends checkpointed data and (next checkpoint begins or metadata in the current cluster end)
    //
    // CRC is calculated from byte sequence | data | checkpointed_data_length |
    // E.g. if the data consist of bytes "abcd" and checkpointed_data_length of bytes "xyz" then the byte sequence
    // would be "abcdxyz"
    uint32_t crc32_code;
    unit_size_t checkpointed_data_length;
} __attribute__((packed));

struct ondisk_next_metadata_cluster {
    cluster_id_t cluster_id; // metadata log continues there
} __attribute__((packed));

template<typename T>
constexpr size_t ondisk_entry_size(const T& entry) noexcept {
    static_assert(std::is_same_v<T, ondisk_next_metadata_cluster>, "ondisk entry size not defined for given type");
    return sizeof(ondisk_type) + sizeof(entry);
}

} // namespace seastar::fs
