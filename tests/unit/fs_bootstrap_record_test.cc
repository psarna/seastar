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
#include "fs/cluster.hh"
#include "fs/crc.hh"
#include "fs_mock_block_device.hh"

#include <boost/crc.hpp>
#include <cstring>
#include <seastar/core/print.hh>
#include <seastar/fs/block_device.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace seastar;
using namespace seastar::fs;

namespace {

inline std::vector<bootstrap_record::shard_info> prepare_valid_shards_info(uint32_t size) {
    std::vector<bootstrap_record::shard_info> ret(size);
    cluster_id_t curr = 1;
    for (bootstrap_record::shard_info& info : ret) {
        info.available_clusters = {curr, curr + 1};
        info.metadata_cluster = curr;
        curr++;
    }
    return ret;
};

inline void repair_crc32(shared_ptr<mock_block_device_impl> dev_impl) noexcept {
    mock_block_device_impl::buf_type& buff = dev_impl.get()->buf;
    constexpr size_t crc_pos = offsetof(bootstrap_record_disk, crc);
    const uint32_t crc_new = crc32(buff.data(), crc_pos);
    std::memcpy(buff.data() + crc_pos, &crc_new, sizeof(crc_new));
}

inline void change_byte_at_offset(shared_ptr<mock_block_device_impl> dev_impl, size_t offset) noexcept {
    dev_impl.get()->buf[offset] ^= 1;
}

template<typename T>
inline void place_at_offset(shared_ptr<mock_block_device_impl> dev_impl, size_t offset, T value) noexcept {
    std::memcpy(dev_impl.get()->buf.data() + offset, &value, sizeof(value));
}

template<>
inline void place_at_offset(shared_ptr<mock_block_device_impl> dev_impl, size_t offset,
        std::vector<bootstrap_record::shard_info> shards_info) noexcept {
    bootstrap_record::shard_info shards_info_disk[bootstrap_record::max_shards_nb];
    std::memset(shards_info_disk, 0, sizeof(shards_info_disk));
    std::copy(shards_info.begin(), shards_info.end(), shards_info_disk);

    std::memcpy(dev_impl.get()->buf.data() + offset, shards_info_disk, sizeof(shards_info_disk));
}

inline bool check_exception_message(const invalid_bootstrap_record& ex, const sstring& message) {
    return sstring(ex.what()).find(message) != sstring::npos;
}

const bootstrap_record default_write_record(1, bootstrap_record::min_alignment * 4,
        bootstrap_record::min_alignment * 8, 1, {{6, {6, 9}}, {9, {9, 12}}, {12, {12, 15}}});

}



BOOST_TEST_DONT_PRINT_LOG_VALUE(bootstrap_record)

SEASTAR_THREAD_TEST_CASE(valid_basic_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    const bootstrap_record write_record = default_write_record;

    write_record.write_to_disk(dev).get();
    const bootstrap_record read_record = bootstrap_record::read_from_disk(dev).get0();
    BOOST_REQUIRE_EQUAL(write_record, read_record);
}

SEASTAR_THREAD_TEST_CASE(valid_max_shards_nb_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    bootstrap_record write_record = default_write_record;
    write_record.shards_info = prepare_valid_shards_info(bootstrap_record::max_shards_nb);

    write_record.write_to_disk(dev).get();
    const bootstrap_record read_record = bootstrap_record::read_from_disk(dev).get0();
    BOOST_REQUIRE_EQUAL(write_record, read_record);
}

SEASTAR_THREAD_TEST_CASE(valid_one_shard_test) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    bootstrap_record write_record = default_write_record;
    write_record.shards_info = prepare_valid_shards_info(1);

    write_record.write_to_disk(dev).get();
    const bootstrap_record read_record = bootstrap_record::read_from_disk(dev).get0();
    BOOST_REQUIRE_EQUAL(write_record, read_record);
}



SEASTAR_THREAD_TEST_CASE(invalid_crc_read) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    const bootstrap_record write_record = default_write_record;

    constexpr size_t crc_offset = offsetof(bootstrap_record_disk, crc);

    write_record.write_to_disk(dev).get();
    change_byte_at_offset(dev_impl, crc_offset);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Invalid CRC");
            });
}

SEASTAR_THREAD_TEST_CASE(invalid_magic_read) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    const bootstrap_record write_record = default_write_record;

    constexpr size_t magic_offset = offsetof(bootstrap_record_disk, magic);

    write_record.write_to_disk(dev).get();
    change_byte_at_offset(dev_impl, magic_offset);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Invalid magic number");
            });
}

SEASTAR_THREAD_TEST_CASE(invalid_shards_info_read) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    const bootstrap_record write_record = default_write_record;

    constexpr size_t shards_nb_offset = offsetof(bootstrap_record_disk, shards_nb);
    constexpr size_t shards_info_offset = offsetof(bootstrap_record_disk, shards_info);

    // shards_nb > max_shards_nb
    write_record.write_to_disk(dev).get();
    place_at_offset(dev_impl, shards_nb_offset, bootstrap_record::max_shards_nb + 1);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, fmt::format("Shards number should be smaller or equal to {}", bootstrap_record::max_shards_nb));
            });

    // shards_nb == 0
    write_record.write_to_disk(dev).get();
    place_at_offset(dev_impl, shards_nb_offset, 0);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Shards number should be greater than 0");
            });

    std::vector<bootstrap_record::shard_info> shards_info;

    // metadata_cluster not in available_clusters range
    write_record.write_to_disk(dev).get();
    shards_info = {{1, {2, 3}}};
    place_at_offset(dev_impl, shards_nb_offset, shards_info.size());
    place_at_offset(dev_impl, shards_info_offset, shards_info);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster with metadata should be inside available cluster range");
            });

    write_record.write_to_disk(dev).get();
    shards_info = {{3, {2, 3}}};
    place_at_offset(dev_impl, shards_nb_offset, shards_info.size());
    place_at_offset(dev_impl, shards_info_offset, shards_info);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster with metadata should be inside available cluster range");
            });

    // available_clusters.beg > available_clusters.end
    write_record.write_to_disk(dev).get();
    shards_info = {{3, {4, 2}}};
    place_at_offset(dev_impl, shards_nb_offset, shards_info.size());
    place_at_offset(dev_impl, shards_info_offset, shards_info);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Invalid cluster range");
            });

    // available_clusters.beg == available_clusters.end
    write_record.write_to_disk(dev).get();
    shards_info = {{2, {2, 2}}};
    place_at_offset(dev_impl, shards_nb_offset, shards_info.size());
    place_at_offset(dev_impl, shards_info_offset, shards_info);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Invalid cluster range");
            });

    // available_clusters contains cluster 0
    write_record.write_to_disk(dev).get();
    shards_info = {{1, {0, 5}}};
    place_at_offset(dev_impl, shards_nb_offset, shards_info.size());
    place_at_offset(dev_impl, shards_info_offset, shards_info);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Range of available clusters should not contain cluster 0");
            });

    // available_clusters overlap
    write_record.write_to_disk(dev).get();
    shards_info = {{1, {1, 3}}, {2, {2, 4}}};
    place_at_offset(dev_impl, shards_nb_offset, shards_info.size());
    place_at_offset(dev_impl, shards_info_offset, shards_info);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster ranges should not overlap");
            });
}

SEASTAR_THREAD_TEST_CASE(invalid_alignment_read) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    const bootstrap_record write_record = default_write_record;

    constexpr size_t alignment_offset = offsetof(bootstrap_record_disk, alignment);

    // alignment not power of 2
    write_record.write_to_disk(dev).get();
    place_at_offset(dev_impl, alignment_offset, bootstrap_record::min_alignment + 1);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Alignment should be a power of 2");
            });

    // alignment smaller than 512
    write_record.write_to_disk(dev).get();
    place_at_offset(dev_impl, alignment_offset, bootstrap_record::min_alignment / 2);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, fmt::format("Alignment should be greater or equal to {}", bootstrap_record::min_alignment));
            });
}

SEASTAR_THREAD_TEST_CASE(invalid_cluster_size_read) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    const bootstrap_record write_record = default_write_record;

    constexpr size_t cluster_size_offset = offsetof(bootstrap_record_disk, cluster_size);

    // cluster_size not divisible by alignment
    write_record.write_to_disk(dev).get();
    place_at_offset(dev_impl, cluster_size_offset, write_record.alignment / 2);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster size should be divisible by alignment");
            });

    // cluster_size not power of 2
    write_record.write_to_disk(dev).get();
    place_at_offset(dev_impl, cluster_size_offset, write_record.alignment * 3);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster size should be a power of 2");
            });
}



SEASTAR_THREAD_TEST_CASE(invalid_shards_info_write) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    bootstrap_record write_record = default_write_record;

    // shards_nb > max_shards_nb
    write_record = default_write_record;
    write_record.shards_info = prepare_valid_shards_info(bootstrap_record::max_shards_nb + 1);
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, fmt::format("Shards number should be smaller or equal to {}", bootstrap_record::max_shards_nb));
            });

    // shards_nb == 0
    write_record = default_write_record;
    write_record.shards_info.clear();
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Shards number should be greater than 0");
            });

    // metadata_cluster not in available_clusters range
    write_record = default_write_record;
    write_record.shards_info = {{1, {2, 3}}};
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster with metadata should be inside available cluster range");
            });

    write_record = default_write_record;
    write_record.shards_info = {{3, {2, 3}}};
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster with metadata should be inside available cluster range");
            });

    // available_clusters.beg > available_clusters.end
    write_record = default_write_record;
    write_record.shards_info = {{3, {4, 2}}};
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Invalid cluster range");
            });

    // available_clusters.beg == available_clusters.end
    write_record = default_write_record;
    write_record.shards_info = {{2, {2, 2}}};
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Invalid cluster range");
            });

    // available_clusters contains cluster 0
    write_record = default_write_record;
    write_record.shards_info = {{1, {0, 5}}};
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Range of available clusters should not contain cluster 0");
            });

    // available_clusters overlap
    write_record = default_write_record;
    write_record.shards_info = {{1, {1, 3}}, {2, {2, 4}}};
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster ranges should not overlap");
            });
}

SEASTAR_THREAD_TEST_CASE(invalid_alignment_write) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    bootstrap_record write_record = default_write_record;

    // alignment not power of 2
    write_record = default_write_record;
    write_record.alignment = bootstrap_record::min_alignment + 1;
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Alignment should be a power of 2");
            });

    // alignment smaller than bootstrap_record::min_alignment
    write_record = default_write_record;
    write_record.alignment = bootstrap_record::min_alignment / 2;
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, fmt::format("Alignment should be greater or equal to {}", bootstrap_record::min_alignment));
            });
}

SEASTAR_THREAD_TEST_CASE(invalid_cluster_size_write) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    bootstrap_record write_record = default_write_record;

    // cluster_size not divisible by alignment
    write_record = default_write_record;
    write_record.cluster_size = write_record.alignment / 2;
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster size should be divisible by alignment");
            });

    // cluster_size not power of 2
    write_record = default_write_record;
    write_record.cluster_size = write_record.alignment * 3;
    BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return check_exception_message(ex, "Cluster size should be a power of 2");
            });
}
