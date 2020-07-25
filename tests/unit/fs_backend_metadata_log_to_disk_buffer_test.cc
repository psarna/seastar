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

#include "fs/backend/metadata_log/entries.hh"
#include "fs/backend/metadata_log/to_disk_buffer.hh"
#include "fs_backend_metadata_log_entries_compare.hh"
#include "fs_backend_metadata_log_entries_print.hh"
#include "fs_backend_metadata_log_entries_random.hh"
#include "fs_backend_metadata_log_random_entry.hh"
#include "fs_block_device_mocker.hh"
#include "fs_random.hh"
#include "seastar/core/print.hh"
#include "seastar/core/units.hh"
#include "seastar/fs/bitwise.hh"
#include "seastar/fs/block_device.hh"
#include "seastar/fs/unit_types.hh"
#include "seastar/testing/thread_test_case.hh"

#include <boost/crc.hpp>
#include <variant>

using std::pair;
using std::string_view;
using std::vector;

using namespace seastar;
using namespace seastar::fs;
namespace ml = seastar::fs::backend::metadata_log;
namespace mle = seastar::fs::backend::metadata_log::entries;

struct test_base {
    static constexpr auto APPENDED = ml::to_disk_buffer::APPENDED;
    static constexpr auto TOO_BIG = ml::to_disk_buffer::TOO_BIG;
    static constexpr auto buf_size = 16 * KB;
    static constexpr size_t alignment = 256;
    const size_t necessary_bytes = mle::ondisk_size<mle::checkpoint>() + mle::ondisk_size<mle::next_metadata_cluster>();

    shared_ptr<block_device_mocker_impl> device_holder;
    block_device_mocker_impl& device;
    block_device bdev;
    ml::to_disk_buffer buf;

    test_base()
    : device_holder(make_shared<block_device_mocker_impl>(alignment))
    , device(*device_holder.get())
    , bdev(device_holder) {
        assert(!buf.init(buf_size, alignment, 0));
    }
};

SEASTAR_THREAD_TEST_CASE(test_too_big) {
    constexpr size_t variable_len_entry_max_len = 20;
    struct tester : test_base {
        void test() {
            size_t bytes_used = mle::ondisk_size<mle::checkpoint>();
            for (;;) {
                if (random_value(0, 100) == 0) {
                    buf.flush_to_disk(bdev).get();
                    bytes_used = round_up_to_multiple_of_power_of_2(bytes_used, alignment);
                    if (bytes_used < buf_size) {
                        bytes_used += mle::ondisk_size<mle::checkpoint>();
                    }
                    continue;
                }

                ml::any_entry entry = ml::random_entry(variable_len_entry_max_len);
                auto ds = ml::disk_size(entry);
                switch (ml::append_entry(buf, entry)) {
                case APPENDED:
                    bytes_used += ds;
                    continue;
                case TOO_BIG:
                    if (!std::holds_alternative<mle::next_metadata_cluster>(entry)) {
                        ds += mle::ondisk_size<mle::next_metadata_cluster>();
                    }
                    if (bytes_used + ds <= buf_size) {
                        std::visit([](auto& e) {
                            std::cerr << "entry: " << e << std::endl;
                        }, entry);
                        BOOST_FAIL(format("{} <= {}: bytes_used={} ds={} buf_size={}",
                                bytes_used + ds, buf_size, bytes_used, ds, buf_size));
                    }
                    return;
                }
            }
        }
    };

    for (size_t rep = 0; rep < 1000; ++rep) {
        tester().test();
    }
}

SEASTAR_THREAD_TEST_CASE(test_writing_entries) {
    constexpr size_t variable_len_entry_max_len = 20;
    struct tester : test_base {
        vector<pair<disk_offset_t, ml::any_entry>> appended_entries;
        vector<disk_offset_t> flushes_offsets;

        void test() {
            disk_offset_t offset = mle::ondisk_size<mle::checkpoint>();
            size_t dev_writes_done = 0;
            for (;;) {
                BOOST_REQUIRE_EQUAL(dev_writes_done, device.writes.size()); // Writes allowed only by flush
                if (random_value(0, 100) == 0) {
                    flushes_offsets.emplace_back(offset);
                    buf.flush_to_disk(bdev).get();
                    dev_writes_done = device.writes.size();
                    offset = round_up_to_multiple_of_power_of_2(offset, alignment);
                    if (offset < buf_size) {
                        offset += mle::ondisk_size<mle::checkpoint>();
                    }
                    continue;
                }

                ml::any_entry entry = ml::random_entry(variable_len_entry_max_len);
                auto ds = ml::disk_size(entry);
                switch (ml::append_entry(buf, entry)) {
                case APPENDED:
                    appended_entries.emplace_back(offset, std::move(entry));
                    offset += ds;
                    break;
                case TOO_BIG:
                    goto break_for;
                }
            }

        break_for:
            flushes_offsets.emplace_back(offset);
            buf.flush_to_disk(bdev).get();

            check_writes_alignment();
            check_checkpoints();
            check_appended_entries();
        }

        void check_writes_alignment() {
            for (auto& write : device.writes) {
                BOOST_REQUIRE_EQUAL(write.disk_offset % alignment, 0);
            }
        }

        void check_checkpoints() {
            disk_range cp_rng = {.beg = 0};
            for (auto flush_offset : flushes_offsets) {
                cp_rng.end = flush_offset;
                if (cp_rng.is_empty()) {
                    continue;
                }

                string_view sv(reinterpret_cast<const char*>(device.buf.data()) + cp_rng.beg, cp_rng.size());
                auto read_res = mle::read<mle::checkpoint>(sv);
                BOOST_REQUIRE(read_res);
                auto& cp = read_res.value();
                BOOST_REQUIRE_EQUAL(cp.checkpointed_data_length, sv.size());
                boost::crc_32_type crc;
                crc.process_bytes(sv.data(), sv.size());
                crc.process_bytes(&cp.checkpointed_data_length, sizeof(cp.checkpointed_data_length));
                BOOST_REQUIRE_EQUAL(cp.crc32_checksum, crc.checksum());

                cp_rng.beg = round_up_to_multiple_of_power_of_2(flush_offset, alignment);
            }
        }

        void check_appended_entries() {
            for (auto& [offset, appended_entry] : appended_entries) {
                mle::any_entry expected_entry = std::visit([&](auto& entry) -> mle::any_entry {
                    return std::move(entry);
                }, appended_entry);

                string_view sv(reinterpret_cast<const char*>(device.buf.data()) + offset, device.buf.size() - offset);
                auto read_res = mle::read_any(sv);
                BOOST_REQUIRE(read_res);
                BOOST_REQUIRE_EQUAL(read_res.value(), expected_entry);
            }
        }
    };

    for (size_t rep = 0; rep < 1000; ++rep) {
        tester().test();
    }
}
