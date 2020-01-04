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

#include "fs/path.hh"

#define BOOST_TEST_MODULE fs
#include <boost/test/included/unit_test.hpp>

using namespace seastar::fs;

BOOST_AUTO_TEST_CASE(last_component_simple) {
    {
        std::string str = "";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), "");
        BOOST_REQUIRE_EQUAL(str, "");
    }
    {
        std::string str = "/";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), "");
        BOOST_REQUIRE_EQUAL(str, "/");
    }
    {
        std::string str = "/foo/bar.txt";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), "bar.txt");
        BOOST_REQUIRE_EQUAL(str, "/foo/");
    }
    {
        std::string str = "/foo/.bar";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), ".bar");
        BOOST_REQUIRE_EQUAL(str, "/foo/");
    }
    {
        std::string str = "/foo/bar/";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), "");
        BOOST_REQUIRE_EQUAL(str, "/foo/bar/");
    }
    {
        std::string str = "/foo/.";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), ".");
        BOOST_REQUIRE_EQUAL(str, "/foo/");
    }
    {
        std::string str = "/foo/..";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), "..");
        BOOST_REQUIRE_EQUAL(str, "/foo/");
    }
    {
        std::string str = "bar.txt";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), "bar.txt");
        BOOST_REQUIRE_EQUAL(str, "");
    }
    {
        std::string str = ".bar";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), ".bar");
        BOOST_REQUIRE_EQUAL(str, "");
    }
    {
        std::string str = ".";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), ".");
        BOOST_REQUIRE_EQUAL(str, "");
    }
    {
        std::string str = "..";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), "..");
        BOOST_REQUIRE_EQUAL(str, "");
    }
    {
        std::string str = "//host";
        BOOST_REQUIRE_EQUAL(extract_last_component(str), "host");
        BOOST_REQUIRE_EQUAL(str, "//");
    }
}
