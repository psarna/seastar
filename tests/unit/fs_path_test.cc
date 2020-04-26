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

using namespace seastar::fs::path;

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

BOOST_AUTO_TEST_CASE(path_canonical_simple) {
    BOOST_REQUIRE_EQUAL(canonical("/foo/bar/"), "/foo/bar/");
    BOOST_REQUIRE_EQUAL(canonical("/foo/bar/////"), "/foo/bar/");
    BOOST_REQUIRE_EQUAL(canonical("/foo/bar/../"), "/foo/");
    BOOST_REQUIRE_EQUAL(canonical("/foo/bar/.."), "/foo/");
    BOOST_REQUIRE_EQUAL(canonical("/foo/bar/xd"), "/foo/bar/xd");
    BOOST_REQUIRE_EQUAL(canonical("/foo/bar/////xd"), "/foo/bar/xd");
    BOOST_REQUIRE_EQUAL(canonical("/foo/bar/../xd"), "/foo/xd");
    BOOST_REQUIRE_EQUAL(canonical("/foo/bar/../xd/."), "/foo/xd/");
    BOOST_REQUIRE_EQUAL(canonical("/foo/bar/./../xd/."), "/foo/xd/");
    BOOST_REQUIRE_EQUAL(canonical("/.."), "/");
    BOOST_REQUIRE_EQUAL(canonical(".."), "/");
    BOOST_REQUIRE_EQUAL(canonical("/////"), "/");
    BOOST_REQUIRE_EQUAL(canonical("////a/////"), "/a/");

    BOOST_REQUIRE_EQUAL(canonical("/my/path/foo.bar"), "/my/path/foo.bar");
    BOOST_REQUIRE_EQUAL(canonical("/my/path/"), "/my/path/");
    BOOST_REQUIRE_EQUAL(canonical("/"), "/");
    BOOST_REQUIRE_EQUAL(canonical(""), "/");
    BOOST_REQUIRE_EQUAL(canonical("/my/path/."), "/my/path/");
    BOOST_REQUIRE_EQUAL(canonical("/my/path/.."), "/my/");
    BOOST_REQUIRE_EQUAL(canonical("foo"), "/foo");
    BOOST_REQUIRE_EQUAL(canonical("/lol/../foo."), "/foo.");
    BOOST_REQUIRE_EQUAL(canonical("/lol/../.foo."), "/.foo.");
    BOOST_REQUIRE_EQUAL(canonical("/.foo."), "/.foo.");
    BOOST_REQUIRE_EQUAL(canonical(".foo."), "/.foo.");
    BOOST_REQUIRE_EQUAL(canonical("./foo."), "/foo.");
    BOOST_REQUIRE_EQUAL(canonical("./f"), "/f");
    BOOST_REQUIRE_EQUAL(canonical("../"), "/");

    BOOST_REQUIRE_EQUAL(canonical("../../a", "foo/bar"), "a");
    BOOST_REQUIRE_EQUAL(canonical("../../a/../b", "foo/bar"), "b");
    BOOST_REQUIRE_EQUAL(canonical("../", "foo/bar"), "foo/");
    BOOST_REQUIRE_EQUAL(canonical("gg", "foo/bar"), "foo/bar/gg");
}

BOOST_AUTO_TEST_CASE(is_path_canonical_simple) {
    BOOST_REQUIRE_EQUAL(is_canonical("/foo/bar/"), true);
    BOOST_REQUIRE_EQUAL(is_canonical("/foo/bar/////"), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/foo/bar/../"), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/foo/bar/.."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/foo/bar/xd"), true);
    BOOST_REQUIRE_EQUAL(is_canonical("/foo/bar/////xd"), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/foo/bar/../xd"), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/foo/bar/../xd/."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/foo/bar/./../xd/."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/.."), false);
    BOOST_REQUIRE_EQUAL(is_canonical(".."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/////"), false);
    BOOST_REQUIRE_EQUAL(is_canonical("////a/////"), false);

    BOOST_REQUIRE_EQUAL(is_canonical("/my/path/foo.bar"), true);
    BOOST_REQUIRE_EQUAL(is_canonical("/my/path/"), true);
    BOOST_REQUIRE_EQUAL(is_canonical("/"), true);
    BOOST_REQUIRE_EQUAL(is_canonical(""), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/my/path/."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/my/path/.."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("foo"), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/lol/../foo."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/lol/../.foo."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("/.foo."), true);
    BOOST_REQUIRE_EQUAL(is_canonical(".foo."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("./foo."), false);
    BOOST_REQUIRE_EQUAL(is_canonical("./f"), false);
    BOOST_REQUIRE_EQUAL(is_canonical("../"), false);
}

BOOST_AUTO_TEST_CASE(first_component_simple) {
    BOOST_REQUIRE_EQUAL(root_entry(""), "");
    BOOST_REQUIRE_EQUAL(root_entry("/"), "");
    BOOST_REQUIRE_EQUAL(root_entry("/foo/bar.txt"), "foo");
    BOOST_REQUIRE_EQUAL(root_entry("/foo/.bar"), "foo");
    BOOST_REQUIRE_EQUAL(root_entry("/foo/bar/"), "foo");
    BOOST_REQUIRE_EQUAL(root_entry("/foo/."), "foo");
    BOOST_REQUIRE_EQUAL(root_entry("/foo/.."), "foo");
    BOOST_REQUIRE_EQUAL(root_entry("bar.txt"), "bar.txt");
    BOOST_REQUIRE_EQUAL(root_entry(".bar"), ".bar");
    BOOST_REQUIRE_EQUAL(root_entry("."), ".");
    BOOST_REQUIRE_EQUAL(root_entry(".."), "..");
    BOOST_REQUIRE_EQUAL(root_entry("//host"), "");
}
