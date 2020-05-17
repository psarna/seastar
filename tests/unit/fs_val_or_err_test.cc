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

#include "fs/val_or_err.hh"

#define BOOST_TEST_MODULE fs
#include <boost/test/included/unit_test.hpp>

using namespace seastar::fs;

using std::pair;
using std::string;

enum Error {
    ERR_A,
    ERR_B,
};

val_or_err<int, Error> f1(int x) {
    if (x == 1) {
        return ERR_A;
    }
    return x;
}

val_or_err<int, Error> f2(int x) {
    if (x == 2) {
        return ERR_B;
    }
    return x;
}

val_or_err<std::pair<int, int>, Error> f12(int x) {
    TRY_VAL(x1, f1(x));
    TRY_VAL(x2, f2(x));
    return std::pair{x1, x2};
}

BOOST_AUTO_TEST_CASE(simple) {
    std::array r = {f12(0), f12(1), f12(2), f12(3)};
    BOOST_REQUIRE(r[0]);
    BOOST_REQUIRE((r[0].value() == pair{0, 0}));

    BOOST_REQUIRE(!r[1]);
    BOOST_REQUIRE(r[1].error() == ERR_A);

    BOOST_REQUIRE(!r[2]);
    BOOST_REQUIRE(r[2].error() == ERR_B);

    BOOST_REQUIRE(r[3]);
    BOOST_REQUIRE((r[3].value() == pair{3, 3}));
}

BOOST_AUTO_TEST_CASE(same_type) {
    constexpr val_or_err<int, int> x;
    static_assert(x);

    constexpr auto y = val_or_err<int, int>::value(42);
    static_assert(y && y.value() == 42);

    constexpr auto z = val_or_err<int, int>::error(42);
    static_assert(!z);
}

BOOST_AUTO_TEST_CASE(constructors) {
    constexpr val_or_err<int, char> x(4);
    static_assert(x);

    constexpr val_or_err<int, char> y('a');
    static_assert(!y);

    val_or_err<int, string> z = "abc abc";
    BOOST_REQUIRE_EQUAL((bool)z, false);
    BOOST_REQUIRE_EQUAL(z.error(), "abc abc");
}

struct move_only {
    int x;
    explicit move_only(int x) : x(x) {}

    move_only(const move_only&) = delete;
    move_only(move_only&&) = default;
};

BOOST_AUTO_TEST_CASE(move_only_type) {
    auto lam = [](int x) -> val_or_err<move_only, string> {
        if (x == 42) {
            return "42 is forbidden";
        }

        move_only foo(x);
        return foo;
    };

    auto r42 = lam(42);
    BOOST_REQUIRE(!r42);
    BOOST_REQUIRE(r42.error() == "42 is forbidden");

    auto r8 = lam(8);
    BOOST_REQUIRE(r8);
    BOOST_REQUIRE(r8.value().x == 8);
}
