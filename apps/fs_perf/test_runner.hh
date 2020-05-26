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

#pragma once

#include "filesystem_mgmt.hh"

#include <algorithm>
#include <boost/range/adaptors.hpp>
#include <chrono>
#include <exception>
#include <iostream>
#include <memory>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/fs/filesystem.hh>
#include <seastar/fs/temporary_directory.hh>
#include <seastar/util/defer.hh>
#include <string>
#include <utility>
#include <vector>

struct sfs_config {
    size_t alignment;
    std::string device_path;
    size_t cluster_size;
};

struct default_config {
    filesystem_type fs_type;
    std::string device_path;
};

namespace internal {

template<typename RunTester, typename RunConfig>
seastar::future<double> measure_run_time(seastar::sharded<RunTester>& tester, RunConfig& rconf) {
    try {
        double ms = seastar::later().then([&] {
            using clock_type = std::chrono::steady_clock;
            clock_type::time_point start_time = clock_type::now();
            return tester.invoke_on_all([](RunTester& local_env) {
                return local_env.run();
            }).then([start_time = std::move(start_time)] {
                auto end_time = clock_type::now();
                return std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            });
        }).get0();
        return seastar::make_ready_future<double>(ms);
    } catch (...) {
        std::cerr << "Exception occurred during run: " << std::current_exception() << std::endl;
        throw;
    }
}

template<typename RunTester, typename RunConfig>
seastar::future<double> start_run(const sfs_config& fsconf, const RunConfig& rconf) {
    seastar::sharded<seastar::fs::filesystem> fs;
    seastar::fs::mkfs(fsconf.device_path, 0, fsconf.cluster_size, fsconf.alignment, 0, seastar::smp::count).get();
    seastar::fs::bootfs(fs, fsconf.device_path).get();
    auto stop_fs = seastar::defer([&] { fs.stop().get(); });

    seastar::sharded<RunTester> tester;
    tester.start(std::ref(fs), rconf).get();
    auto stop_tester = seastar::defer([&tester] {
        tester.stop().get();
    });

    try {
        tester.invoke_on_all([](RunTester& local_env) {
            return local_env.init();
        }).get();
    } catch (...) {
        std::cerr << "Exception occurred during run init: " << std::current_exception() << std::endl;
        throw;
    }

    return measure_run_time(tester, rconf);
}

template<typename RunTester, typename RunConfig>
seastar::future<double> start_run(const default_config& fsconf, const RunConfig& rconf) {
    mkfs(fsconf.device_path, fsconf.fs_type);
    seastar::fs::temporary_directory mount_point("/tmp/.fs_perf");
    mount(fsconf.device_path, mount_point.path());
    auto unmount_fs = seastar::defer([&] { unmount(mount_point.path()); });

    seastar::sharded<RunTester> tester;
    tester.start(mount_point.path(), rconf).get();
    auto stop_tester = seastar::defer([&tester] {
        tester.stop().get();
    });

    try {
        tester.invoke_on_all([](RunTester& local_env) {
            return local_env.init();
        }).get();
    } catch (...) {
        std::cerr << "Exception occurred during run init: " << std::current_exception() << std::endl;
        throw;
    }

    return measure_run_time(tester, rconf);
}

}

struct result {
    std::string test_name;

    unsigned runs;

    double median;
    double mad;
    double min;
    double max;
};

class result_printer {
public:
    virtual ~result_printer() = default;

    virtual void print_configuration() = 0;
    virtual void print_result(const result&) = 0;
};

template<typename RunTester, typename FsConfig, typename RunConfig>
seastar::future<> start_test(const std::string& name, size_t runs_nb, const FsConfig& fsconf, const RunConfig& rconf,
        const std::vector<std::unique_ptr<result_printer>>& result_printers) {
    std::vector<double> results(runs_nb);
    for (size_t i = 0; i < runs_nb; ++i) {
        ::internal::start_run<RunTester>(fsconf, rconf).then([&](double ms) {
            results[i] = ms;
        }).get();
    }

    result r;
    r.test_name = std::move(name);
    r.runs = runs_nb;

    auto mid = runs_nb / 2;

    std::sort(results.begin(), results.end());
    r.median = (results[mid] + results[results.size() - 1 - mid]) / 2;

    auto diffs = boost::copy_range<std::vector<double>>(
        results | boost::adaptors::transformed([&] (double x) { return fabs(x - r.median); })
    );
    std::sort(diffs.begin(), diffs.end());
    r.mad = diffs[mid];

    r.min = results[0];
    r.max = results[results.size() - 1];

    for (auto& rp : result_printers) {
        rp->print_result(r);
    }

    return seastar::now();
}
