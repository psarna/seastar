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

#include "fs/metadata_log/entries.hh"
#include "fs_metadata_log_entries_compare.hh"
#include "fs_random.hh"
#include "seastar/core/print.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/testing/test_runner.hh"
#include "seastar/util/defer.hh"

#include <cstdint>
#include <limits>
#include <random>
#include <string_view>
#include <type_traits>
#include <variant>

#define BOOST_TEST_MODULE fs
#include <boost/test/included/unit_test.hpp>

namespace mle = seastar::fs::metadata_log::entries;
using namespace seastar::fs;
using namespace seastar;

using std::holds_alternative;
using std::string;
using std::string_view;

template<class Entry>
void check_underlying_bits(const Entry& entry, string buff) {
    for (size_t i = 0; i < buff.size(); ++i) {
        for (int bit = 0; bit < 8; ++bit) {
            buff[i] ^= 1 << bit;
            deferred_action undo = [&] {
                buff[i] ^= 1 << bit;
            };
            // mle::read
            {
                string_view sv(buff);
                auto res = mle::read<Entry>(sv);
                if (res && res.value() == entry) {
                    BOOST_FAIL(format("byte {} at bit {}", i, bit));
                }
                if (!res) {
                    BOOST_REQUIRE_EQUAL(sv.size(), buff.size());
                    BOOST_REQUIRE_EQUAL(sv.data(), buff.data());
                }
            }
            // mle::read_any
            {
                string_view sv(buff);
                auto res = mle::read_any(sv);
                if (res && holds_alternative<Entry>(res.value()) && std::get<Entry>(res.value()) == entry) {
                    BOOST_FAIL(format("byte {} at bit {}", i, bit));
                }
                if (!res) {
                    BOOST_REQUIRE_EQUAL(sv.size(), buff.size());
                    BOOST_REQUIRE_EQUAL(sv.data(), buff.data());
                }
            }
        }
    }
}

template<class Entry>
void entry_test(const Entry& entry, size_t entry_size) {
    BOOST_REQUIRE_EQUAL(entry_size, mle::ondisk_size(entry));
    string buff(entry_size, '\0');
    random_overwrite(buff.data(), buff.size());
    mle::write(entry, buff.data());

    for (size_t size = entry_size; size < entry_size * 2 + 10; ++size) {
        buff.resize(size);
        buff.shrink_to_fit(); // to potentially expose invalid memory accesses
        // mle::read
        {
            string_view sv = buff;
            auto res = mle::read<Entry>(sv);
            BOOST_REQUIRE_MESSAGE(res, format("entry_size: {}, size: {}", entry_size, size));
            auto& v = res.value();
            BOOST_REQUIRE_MESSAGE(entry == v, format("entry_size: {}, size: {}", entry_size, size));
            BOOST_REQUIRE(sv.size() == size - entry_size);
            BOOST_REQUIRE(sv.data() == buff.data() + entry_size);
        }
        // mle::read_any
        {
            string_view sv = buff;
            auto res = mle::read_any(sv);
            BOOST_REQUIRE_MESSAGE(res, format("entry_size: {}, size: {}", entry_size, size));
            auto& v = res.value();
            BOOST_REQUIRE_MESSAGE(holds_alternative<Entry>(v), format("entry_size: {}, size: {}", entry_size, size));
            BOOST_REQUIRE_MESSAGE(entry == std::get<Entry>(v), format("entry_size: {}, size: {}", entry_size, size));
            BOOST_REQUIRE(sv.size() == size - entry_size);
            BOOST_REQUIRE(sv.data() == buff.data() + entry_size);
        }
    }

    for (int size = entry_size - 1; size >= 0; --size) {
        buff.resize(size);
        buff.shrink_to_fit(); // to potentially expose invalid memory accesses
        // mle::read
        {
            string_view sv = buff;
            auto res = mle::read<Entry>(sv);
            BOOST_REQUIRE_MESSAGE(!res, format("entry_size: {}, size: {}", entry_size, size));
            BOOST_REQUIRE_MESSAGE(res.error() == mle::TOO_SMALL, format("entry_size: {}, size: {}", entry_size, size));
            BOOST_REQUIRE_EQUAL(sv.size(), buff.size());
            BOOST_REQUIRE_EQUAL(sv.data(), buff.data());
        }
        // mle::read_any
        {
            string_view sv = buff;
            auto res = mle::read_any(sv);
            BOOST_REQUIRE_MESSAGE(!res, format("entry_size: {}, size: {}", entry_size, size));
            BOOST_REQUIRE_MESSAGE(res.error() == mle::TOO_SMALL, format("entry_size: {}, size: {}", entry_size, size));
            BOOST_REQUIRE_EQUAL(sv.size(), buff.size());
            BOOST_REQUIRE_EQUAL(sv.data(), buff.data());
        }
    }

    buff.resize(entry_size);
    random_overwrite(buff.data(), buff.size());
    mle::write(entry, buff.data());
    check_underlying_bits(entry, buff);
}

template<class Entry>
void simple_entry_test() {
    size_t entry_size = mle::ondisk_size<Entry>();
    Entry entry;
    random_overwrite(&entry, sizeof(entry));
    entry_test(entry, entry_size);
}

template<class Bstr>
static Bstr random_bstr(size_t size) {
    if constexpr (std::is_same_v<Bstr, std::string>) {
        Bstr res(size, '\0');
        random_overwrite(res.data(), size);
        return res;
    } else {
        Bstr res(size);
        random_overwrite(res.get_write(), size);
        return res;
    }
}

template<class Entry, class Bstr>
void random_overwrite(Entry& entry, Bstr* (*get_bstr)(Entry&)) {
    static_assert(std::is_standard_layout_v<Entry>);
    uint8_t* entry_ptr = reinterpret_cast<uint8_t*>(&entry);
    size_t bstr_offset = reinterpret_cast<uint8_t*>(get_bstr(entry)) - entry_ptr;
    random_overwrite(entry_ptr, bstr_offset);
    random_overwrite(entry_ptr + bstr_offset + sizeof(Bstr), sizeof(entry) - bstr_offset - sizeof(Bstr));
}

template<class Entry, class Bstr>
void entry_with_bstr_test(size_t max_bstr_size, Bstr* (*get_bstr)(Entry&)) {
    auto test_range = [&](size_t first, size_t last) {
        for (size_t bstr_size = first; bstr_size <= last; ++bstr_size) {
            size_t entry_size = mle::ondisk_size<Entry>(bstr_size);
            Entry entry;
            random_overwrite(entry, get_bstr);
            *get_bstr(entry) = random_bstr<Bstr>(bstr_size);
            entry_test(entry, entry_size);
        }
    };
    test_range(0, 16);
    test_range(max_bstr_size - 1, max_bstr_size);
}

BOOST_AUTO_TEST_CASE(mle_checkpoint) {
    simple_entry_test<mle::checkpoint>();
}

BOOST_AUTO_TEST_CASE(mle_next_metadata_cluster) {
    simple_entry_test<mle::next_metadata_cluster>();
}

BOOST_AUTO_TEST_CASE(mle_create_inode) {
    simple_entry_test<mle::create_inode>();
}

BOOST_AUTO_TEST_CASE(mle_delete_inode) {
    simple_entry_test<mle::delete_inode>();
}

BOOST_AUTO_TEST_CASE(mle_small_write) {
    entry_with_bstr_test<mle::small_write, temporary_buffer<uint8_t>>(mle::small_write::data_max_len,
            [](mle::small_write& e) {
        return &e.data;
    });
}

BOOST_AUTO_TEST_CASE(mle_medium_write) {
    simple_entry_test<mle::medium_write>();
}

BOOST_AUTO_TEST_CASE(mle_large_write_without_time) {
    simple_entry_test<mle::large_write_without_time>();
}

BOOST_AUTO_TEST_CASE(mle_large_write) {
    simple_entry_test<mle::large_write>();
}

BOOST_AUTO_TEST_CASE(mle_truncate) {
    simple_entry_test<mle::truncate>();
}

BOOST_AUTO_TEST_CASE(mle_create_dentry) {
    entry_with_bstr_test<mle::create_dentry, std::string>(mle::dentry_name_max_len, [](mle::create_dentry& e) {
        return &e.name;
    });
}

BOOST_AUTO_TEST_CASE(mle_create_inode_as_dentry) {
    entry_with_bstr_test<mle::create_inode_as_dentry, std::string>(mle::dentry_name_max_len,
            [](mle::create_inode_as_dentry& e) {
        return &e.name;
    });
}

BOOST_AUTO_TEST_CASE(mle_delete_dentry) {
    entry_with_bstr_test<mle::delete_dentry, std::string>(mle::dentry_name_max_len, [](mle::delete_dentry& e) {
        return &e.name;
    });
}

BOOST_AUTO_TEST_CASE(mle_delete_inode_and_dentry) {
    entry_with_bstr_test<mle::delete_inode_and_dentry, std::string>(mle::dentry_name_max_len,
            [](mle::delete_inode_and_dentry& e) {
        return &e.dd.name;
    });
}
