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


#include "fs/backend/read.hh"
#include "fs/backend/truncate.hh"
#include "fs/backend/write.hh"
#include "fs/metadata_log/entries.hh"
#include "fs_backend_shard_tester.hh"
#include "fs_random.hh"
#include "seastar/testing/thread_test_case.hh"

#include <string_view>

using std::string;
using std::string_view;

using namespace seastar;
using namespace seastar::fs;
namespace mle = seastar::fs::metadata_log::entries;

SEASTAR_THREAD_TEST_CASE(test_truncate_exceptions) {
    backend::shard_tester st;
    inode_t inode = st.create_and_open_file();
    const file_offset_t size = random_value(1, 1 * MB);
    BOOST_CHECK_THROW(st.shard.truncate(inode + 1, size).get0(), invalid_inode_exception);
}

SEASTAR_THREAD_TEST_CASE(test_empty_truncate) {
    backend::shard_tester st;
    const file_offset_t size = random_value(1, 1 * MB);
    const inode_t inode = st.create_and_open_file();
    auto& ml_buff = st.curr_ml_buff();

    const auto time_ns = st.clock.refreeze_time_ns();
    st.shard.truncate(inode, size).get0();
    BOOST_TEST_MESSAGE("ml_buff->actions: " << ml_buff.actions);

    // Check metadata
    BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 2);
    BOOST_REQUIRE_EQUAL(ml_buff.actions.back(), (mle::truncate{
        .inode = inode,
        .size = size,
        .time_ns = time_ns
    }));
    // Check data
    BOOST_REQUIRE_EQUAL(st.read(inode, 0, size), string(size, '\0'));
    BOOST_REQUIRE_EQUAL(st.shard.file_size(inode), size);
}

SEASTAR_THREAD_TEST_CASE(test_truncate_to_less) {
    backend::shard_tester st;
    const inode_t inode = st.create_and_open_file();
    auto& ml_buff = st.curr_ml_buff();

    constexpr auto write_offset = 10;
    const auto write_len = 3 * st.options.alignment;
    string written(write_len, 'a');
    st.write(inode, write_offset, written.data(), write_len);
    auto actions_size_after_write = ml_buff.actions.size();

    const auto time_ns = st.clock.refreeze_time_ns();
    constexpr auto size = 77;
    st.shard.truncate(inode, size).get0();
    BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);

    // Check metadata
    BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), actions_size_after_write + 1);
    BOOST_REQUIRE_EQUAL(ml_buff.actions.back(), (mle::truncate{
        .inode = inode,
        .size = size,
        .time_ns = time_ns
    }));
    // Check data
    written.resize(size - write_offset);
    BOOST_REQUIRE_EQUAL(st.read(inode, write_offset, size - write_offset), written);
    BOOST_REQUIRE_EQUAL(st.shard.file_size(inode), size);
}

SEASTAR_THREAD_TEST_CASE(test_truncate_to_more) {
    backend::shard_tester st;
    const inode_t inode = st.create_and_open_file();
    auto& ml_buff = st.curr_ml_buff();

    constexpr auto write_offset = 10;
    const auto write_len = 3 * st.options.alignment;
    auto size = write_len + st.options.alignment / 3;
    string written(write_len, 'a');
    st.write(inode, write_offset, written.data(), write_len);
    auto actions_size_after_write = ml_buff.actions.size();

    const auto time_ns = st.clock.refreeze_time_ns();
    st.shard.truncate(inode, size).get0();
    BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);

    // Check metadata
    BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), actions_size_after_write + 1);
    BOOST_REQUIRE_EQUAL(ml_buff.actions.back(), (mle::truncate{
        .inode = inode,
        .size = size,
        .time_ns = time_ns
    }));
    // Check data
    written.resize(size - write_offset, '\0');
    BOOST_REQUIRE_EQUAL(st.read(inode, write_offset, size - write_offset), written);
    BOOST_REQUIRE_EQUAL(st.shard.file_size(inode), size);
}
