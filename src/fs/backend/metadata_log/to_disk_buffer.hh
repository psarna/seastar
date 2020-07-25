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

#include "fs/backend/metadata_log/entries.hh"
#include "fs/to_disk_buffer.hh"
#include "seastar/fs/bitwise.hh"

#include <boost/crc.hpp>
#include <limits>

namespace seastar::fs::backend::metadata_log {

// Represents buffer that will be written to a block_device. Method init() should be called just after constructor
// in order to finish construction.
class to_disk_buffer : protected fs::to_disk_buffer {
    boost::crc_32_type _crc;

public:
    to_disk_buffer() = default;

    // Total number of bytes appended cannot exceed @p aligned_max_size.
    // @p cluster_beg_offset is the disk offset of the beginning of the cluster.
    std::optional<to_disk_buffer::init_error> init(disk_offset_t aligned_max_size, disk_offset_t alignment,
            disk_offset_t cluster_beg_offset) override {
        namespace mle = metadata_log::entries;
        if (aligned_max_size > std::numeric_limits<decltype(mle::checkpoint::checkpointed_data_length)>::max()) {
            return fs::to_disk_buffer::init_error::MAX_SIZE_TOO_BIG;
        }
        return fs::to_disk_buffer::init(aligned_max_size, alignment, cluster_beg_offset);
    }

    virtual shared_ptr<to_disk_buffer> virtual_constructor() const {
        return make_shared<to_disk_buffer>();
    }

    enum [[nodiscard]] init_error {
        ALIGNMENT_IS_NOT_2_POWER,
        MAX_SIZE_IS_NOT_ALIGNED,
        CLUSTER_BEG_OFFSET_IS_NOT_ALIGNED,
        ALIGNMENT_TOO_SMALL,
        MAX_SIZE_TOO_SMALL,
        MAX_SIZE_TOO_BIG,
        METADADA_END_POS_TOO_BIG,
    };

    /**
     * @brief Inits object, leaving it in state as if just after flushing with unflushed data end at
     *   @p cluster_beg_offset
     *
     * @param aligned_max_size size of the buffer, must be aligned
     * @param alignment write alignment
     * @param cluster_beg_offset disk offset of the beginning of the cluster
     * @param metadata_end_pos position at which valid metadata ends: valid metadata range: [0, @p metadata_end_pos)
     */
    virtual std::optional<init_error> init_from_bootstrapped_cluster(size_t aligned_max_size, disk_offset_t alignment,
            disk_offset_t cluster_beg_offset, size_t metadata_end_pos) {
        namespace mle = metadata_log::entries;
        if (!is_power_of_2(alignment)) {
            return ALIGNMENT_IS_NOT_2_POWER;
        }
        if (mod_by_power_of_2(aligned_max_size, alignment) != 0) {
            return MAX_SIZE_IS_NOT_ALIGNED;
        }
        if (mod_by_power_of_2(cluster_beg_offset, alignment) != 0) {
            return CLUSTER_BEG_OFFSET_IS_NOT_ALIGNED;
        }
        if (aligned_max_size > std::numeric_limits<decltype(_buff.size())>::max()) {
            return MAX_SIZE_TOO_BIG;
        }
        if (aligned_max_size > std::numeric_limits<decltype(mle::checkpoint::checkpointed_data_length)>::max()) {
            return MAX_SIZE_TOO_BIG;
        }
        if (alignment < mle::ondisk_size<mle::checkpoint>() + mle::ondisk_size<mle::next_metadata_cluster>()) {
            // We always need to be able to pack at least a checkpoint and next_metadata_cluster entry to the last
            // data flush in the cluster
            return ALIGNMENT_TOO_SMALL;
        }
        if (aligned_max_size < alignment) {
            return MAX_SIZE_TOO_SMALL;
        }
        if (metadata_end_pos >= aligned_max_size) {
            return METADADA_END_POS_TOO_BIG;
        }

        _max_size = aligned_max_size;
        _alignment = alignment;
        _cluster_beg_offset = cluster_beg_offset;
        auto aligned_pos = round_up_to_multiple_of_power_of_2(metadata_end_pos, _alignment);
        _unflushed_data = {
            .beg = aligned_pos,
            .end = aligned_pos,
        };
        _buff = decltype(_buff)::aligned(_alignment, _max_size);

        start_new_unflushed_data();
        return std::nullopt;
    }

    using fs::to_disk_buffer::flush_to_disk;

protected:
    void start_new_unflushed_data() noexcept override {
        namespace mle = metadata_log::entries;
        if (bytes_left() < mle::ondisk_size<mle::checkpoint>() + mle::ondisk_size<mle::next_metadata_cluster>()) {
            assert(bytes_left() == 0); // alignment has to be big enough to hold mle::checkpoint and next_metadata_cluster
            return; // No more space
        }

        mle::checkpoint cp;
        cp.checkpointed_data_length = 0;
        // set invalid crc code, so that boostraping will stop there unless we correct it
        boost::crc_32_type crc;
        crc.process_bytes(&cp.checkpointed_data_length, sizeof(cp.checkpointed_data_length));
        cp.crc32_checksum = crc.checksum() ^ 42;

        write(cp, get_write());
        fs::to_disk_buffer::acknowledge_write(mle::ondisk_size(cp));

        _crc.reset();
    }

    void prepare_unflushed_data_for_flush() noexcept override {
        namespace mle = metadata_log::entries;
        // Make checkpoint valid
        mle::checkpoint cp;
        cp.checkpointed_data_length = _unflushed_data.size() - mle::ondisk_size<decltype(cp)>();
        _crc.process_bytes(&cp.checkpointed_data_length, sizeof(cp.checkpointed_data_length));
        cp.crc32_checksum = _crc.checksum();

        mle::write(cp, get_write() - _unflushed_data.size());
    }

    void acknowledge_write(size_t len) noexcept override {
        _crc.process_bytes(get_write(), len);
        fs::to_disk_buffer::acknowledge_write(len);
    }

public:
    // Explicitly stated that stays the same
    using fs::to_disk_buffer::bytes_left;
    using fs::to_disk_buffer::bytes_left_after_flush_if_done_now;

    enum [[nodiscard]] append_result {
        APPENDED,
        TOO_BIG,
    };

    virtual append_result append(const metadata_log::entries::next_metadata_cluster& entry) noexcept {
        namespace mle = metadata_log::entries;
        auto dsz = mle::ondisk_size(entry);
        if (bytes_left() < dsz) {
            return TOO_BIG;
        }

        mle::write(entry, get_write());
        acknowledge_write(dsz);
        return APPENDED;
    }

protected:
    bool fits_for_append(size_t bytes_no) const noexcept {
        namespace mle = metadata_log::entries;
        // We need to reserve space for the mle::next_metadata_cluster entry
        return (bytes_left() >= bytes_no + mle::ondisk_size<mle::next_metadata_cluster>());
    }

private:
    template<class T>
    append_result append_entry(const T& entry) noexcept {
        namespace mle = metadata_log::entries;
        auto dsz = mle::ondisk_size(entry);
        if (!fits_for_append(dsz)) {
            return TOO_BIG;
        }

        mle::write(entry, get_write());
        acknowledge_write(dsz);
        return APPENDED;
    }

public:
    virtual append_result append(const metadata_log::entries::create_inode& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::delete_inode& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::small_write& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::medium_write& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::large_write& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::large_write_without_time& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::truncate& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::create_dentry& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::create_inode_as_dentry& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::delete_dentry& entry) noexcept {
        return append_entry(entry);
    }
    virtual append_result append(const metadata_log::entries::delete_inode_and_dentry& entry) noexcept {
        return append_entry(entry);
    }
};

} // namespace seastar::fs::backend::metadata_log
