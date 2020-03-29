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

#include "fs/bitwise.hh"
#include "fs/metadata_disk_entries.hh"
#include "fs/to_disk_buffer.hh"

#include <boost/crc.hpp>

namespace seastar::fs {

// Represents buffer that will be written to a block_device. Method init() should be called just after constructor
// in order to finish construction.
class metadata_to_disk_buffer : protected to_disk_buffer {
    boost::crc_32_type _crc;

public:
    metadata_to_disk_buffer() = default;

    using to_disk_buffer::init; // Explicitly stated that stays the same

    virtual shared_ptr<metadata_to_disk_buffer> virtual_constructor() const {
        return make_shared<metadata_to_disk_buffer>();
    }

    /**
     * @brief Inits object, leaving it in state as if just after flushing with unflushed data end at
     *   @p cluster_beg_offset
     *
     * @param aligned_max_size size of the buffer, must be aligned
     * @param alignment write alignment
     * @param cluster_beg_offset disk offset of the beginning of the cluster
     * @param metadata_end_pos position at which valid metadata ends: valid metadata range: [0, @p metadata_end_pos)
     */
    virtual void init_from_bootstrapped_cluster(size_t aligned_max_size, unit_size_t alignment,
            disk_offset_t cluster_beg_offset, size_t metadata_end_pos) {
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
        _buff = decltype(_buff)::aligned(_alignment, _max_size);

        start_new_unflushed_data();
    }

protected:
    void start_new_unflushed_data() noexcept override {
        if (bytes_left() < sizeof(ondisk_type) + sizeof(ondisk_checkpoint) + sizeof(ondisk_type) +
                sizeof(ondisk_next_metadata_cluster)) {
            assert(bytes_left() == 0); // alignment has to be big enough to hold checkpoint and next_metadata_cluster
            return; // No more space
        }

        ondisk_type type = INVALID;
        ondisk_checkpoint checkpoint;
        std::memset(&checkpoint, 0, sizeof(checkpoint));

        to_disk_buffer::append_bytes(&type, sizeof(type));
        to_disk_buffer::append_bytes(&checkpoint, sizeof(checkpoint));

        _crc.reset();
    }

    void prepare_unflushed_data_for_flush() noexcept override {
        // Make checkpoint valid
        constexpr ondisk_type checkpoint_type = CHECKPOINT;
        size_t checkpoint_pos = _unflushed_data.beg + sizeof(checkpoint_type);
        ondisk_checkpoint checkpoint;
        checkpoint.checkpointed_data_length = _unflushed_data.end - checkpoint_pos - sizeof(checkpoint);
        _crc.process_bytes(&checkpoint.checkpointed_data_length, sizeof(checkpoint.checkpointed_data_length));
        checkpoint.crc32_code = _crc.checksum();

        std::memcpy(_buff.get_write() + _unflushed_data.beg, &checkpoint_type, sizeof(checkpoint_type));
        std::memcpy(_buff.get_write() + checkpoint_pos, &checkpoint, sizeof(checkpoint));
    }

public:
    using to_disk_buffer::bytes_left_after_flush_if_done_now; // Explicitly stated that stays the same

private:
    void append_bytes(const void* data, size_t len) noexcept override {
        to_disk_buffer::append_bytes(data, len);
        _crc.process_bytes(data, len);
    }

public:
    enum append_result {
        APPENDED,
        TOO_BIG,
    };

    [[nodiscard]] virtual append_result append(const ondisk_next_metadata_cluster& next_metadata_cluster) noexcept {
        ondisk_type type = NEXT_METADATA_CLUSTER;
        if (bytes_left() < ondisk_entry_size(next_metadata_cluster)) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&next_metadata_cluster, sizeof(next_metadata_cluster));
        return APPENDED;
    }

    using to_disk_buffer::bytes_left;

protected:
    bool fits_for_append(size_t bytes_no) const noexcept {
        // We need to reserve space for the next metadata cluster entry
        return (bytes_left() >= bytes_no + sizeof(ondisk_type) + sizeof(ondisk_next_metadata_cluster));
    }

private:
    template<class T>
    [[nodiscard]] append_result append_simple(ondisk_type type, const T& entry) noexcept {
        if (not fits_for_append(ondisk_entry_size(entry))) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&entry, sizeof(entry));
        return APPENDED;
    }

public:
    [[nodiscard]] virtual append_result append(const ondisk_create_inode& create_inode) noexcept {
        // TODO: maybe add a constexpr static field to each ondisk_* entry specifying what type it is?
        return append_simple(CREATE_INODE, create_inode);
    }

    [[nodiscard]] virtual append_result append(const ondisk_delete_inode& delete_inode) noexcept {
        return append_simple(DELETE_INODE, delete_inode);
    }

    [[nodiscard]] virtual append_result append(const ondisk_add_dir_entry_header& add_dir_entry, const void* entry_name) noexcept {
        ondisk_type type = ADD_DIR_ENTRY;
        if (not fits_for_append(ondisk_entry_size(add_dir_entry))) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&add_dir_entry, sizeof(add_dir_entry));
        append_bytes(entry_name, add_dir_entry.entry_name_length);
        return APPENDED;
    }

    [[nodiscard]] virtual append_result append(const ondisk_create_inode_as_dir_entry_header& create_inode_as_dir_entry,
            const void* entry_name) noexcept {
        ondisk_type type = CREATE_INODE_AS_DIR_ENTRY;
        if (not fits_for_append(ondisk_entry_size(create_inode_as_dir_entry))) {
            return TOO_BIG;
        }

        append_bytes(&type, sizeof(type));
        append_bytes(&create_inode_as_dir_entry, sizeof(create_inode_as_dir_entry));
        append_bytes(entry_name, create_inode_as_dir_entry.entry_name_length);
        return APPENDED;
    }

    using to_disk_buffer::flush_to_disk;
};

} // namespace seastar::fs
