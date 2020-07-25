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
#include "fs_backend_metadata_log_entries_random.hh"
#include "fs_backend_metadata_log_random_entry.hh"
#include "fs_backend_metadata_log_to_disk_buffer_mocker.hh"
#include "fs_block_device_mocker.hh"
#include "fs_random.hh"
#include "seastar/core/units.hh"
#include "seastar/fs/block_device.hh"
#include "seastar/testing/thread_test_case.hh"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>
#include <variant>

using std::vector;

using namespace seastar;
using namespace seastar::fs;
namespace ml = seastar::fs::backend::metadata_log;
namespace mle = seastar::fs::backend::metadata_log::entries;

struct test_base {
    static constexpr auto APPENDED = ml::to_disk_buffer_mocker::APPENDED;
    static constexpr auto TOO_BIG = ml::to_disk_buffer_mocker::TOO_BIG;
    static constexpr auto alignment = 256;
    static constexpr auto buff_size = 1 * MB;

    block_device device = block_device_mocker(alignment);
    shared_ptr<std::vector<shared_ptr<ml::to_disk_buffer_mocker>>> mbufs_holder;
    std::vector<shared_ptr<ml::to_disk_buffer_mocker>>& mbufs;
    ml::to_disk_buffer buf;
    ml::to_disk_buffer_mocker mock_buf;

    test_base()
    : mbufs_holder(make_shared<decltype(mbufs_holder)::element_type>())
    , mbufs(*mbufs_holder.get())
    , buf()
    , mock_buf(mbufs_holder.get()) {
        assert(!buf.init(buff_size, alignment, 0));
        assert(!mock_buf.init(buff_size, alignment, 0));
    }
};

// The following test checks if exceeding append is properly handled
SEASTAR_THREAD_TEST_CASE(test_too_big) {
    constexpr size_t variable_len_entry_max_len = 20;
    struct tester : test_base {
        void test() {
            bool ok = true;
            while (ok) {
                if (random_value(0, 100) == 0) {
                    buf.flush_to_disk(device).get();
                    mock_buf.flush_to_disk(device).get();
                } else {
                    ml::any_entry entry = ml::random_entry(variable_len_entry_max_len);
                    auto app_res = ml::append_entry(buf, entry);
                    BOOST_REQUIRE_EQUAL(app_res, ml::append_entry(mock_buf, entry));
                    switch (app_res) {
                    case APPENDED: break;
                    case TOO_BIG: ok = false; break;
                    }
                }

                BOOST_REQUIRE_EQUAL(buf.bytes_left(), mock_buf.bytes_left());
            }
        }
    };

    for (size_t rep = 0; rep < 400; ++rep) {
        tester().test();
    }
}

// The following test checks if multiple actions and their info are correctly added to actions vector
SEASTAR_THREAD_TEST_CASE(test_writing_entries) {
    constexpr size_t variable_len_entry_max_len = 20;
    struct tester : test_base {
        vector<ml::mock_entry> expected_entries;

        void test() {
            bool ok = true;
            while (ok) {
                if (random_value(0, 100) == 0) {
                    mock_buf.flush_to_disk(device).get();
                    expected_entries.emplace_back(ml::mock_flush_entry{});
                } else {
                    ml::any_entry entry = ml::random_entry(variable_len_entry_max_len);
                    switch (ml::append_entry(mock_buf, entry)) {
                    case APPENDED:
                        expected_entries.emplace_back(std::visit([](auto& e) -> ml::mock_entry {
                           return std::move(e);
                        }, entry));
                        break;
                    case TOO_BIG:
                        BOOST_TEST_MESSAGE("appending failed: entry is too big");
                        ok = false;
                        break;
                    }
                }

                BOOST_TEST_MESSAGE("last entry: " << expected_entries.back());
                BOOST_REQUIRE_EQUAL(mock_buf.actions.size(), expected_entries.size());
                BOOST_REQUIRE_EQUAL(mock_buf.actions.back(), expected_entries.back());
            }
            // Check that all actions are still there, with correct info
            for (size_t i = 0; i < expected_entries.size(); ++i) {
                BOOST_REQUIRE_EQUAL(mock_buf.actions[i], expected_entries[i]);
            }
        }
    };

    for (size_t rep = 0; rep < 400; ++rep) {
        tester().test();
    }
}

// The folowing test checks that constructed buffers are distinct and correctly added to
// virtually_constructed_buffers vector
SEASTAR_THREAD_TEST_CASE(virtual_constructor_test) {
    struct tester : test_base {
        void test() {
            auto mock_buf0 = mock_buf.virtual_constructor();
            mock_buf0->init(buff_size, alignment, 0);
            BOOST_REQUIRE_EQUAL(mbufs.size(), 1);
            BOOST_REQUIRE(mbufs[0] == mock_buf0);

            auto mock_buf1 = mock_buf0->virtual_constructor();
            mock_buf1->init(buff_size, alignment, 0);
            BOOST_REQUIRE_EQUAL(mbufs.size(), 2);
            BOOST_REQUIRE(mbufs[1] == mock_buf1);

            auto mock_buf2 = mock_buf1->virtual_constructor();
            mock_buf2->init(buff_size, alignment, 0);
            BOOST_REQUIRE_EQUAL(mbufs.size(), 3);
            BOOST_REQUIRE(mbufs[2] == mock_buf2);

            BOOST_REQUIRE_EQUAL(mbufs[0]->actions.size(), 0);

            BOOST_REQUIRE_EQUAL(mock_buf1->append(mle::delete_inode{}), APPENDED);
            BOOST_REQUIRE_EQUAL(mbufs[1]->actions.size(), 1);

            BOOST_REQUIRE_EQUAL(mock_buf2->append(mle::delete_inode{}), APPENDED);
            BOOST_REQUIRE_EQUAL(mock_buf2->append(mle::delete_inode{}), APPENDED);
            BOOST_REQUIRE_EQUAL(mbufs[2]->actions.size(), 2);
        }
    };

    tester().test();
}
