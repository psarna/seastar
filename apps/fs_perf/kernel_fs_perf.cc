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

#include "conversions.hh"
#include "filesystem_mgmt.hh"
#include "fs_tester.hh"
#include "results_printers.hh"
#include "test_runner.hh"

#include <cassert>
#include <fmt/format.h>
#include <memory>
#include <optional>
#include <seastar/core/app-template.hh>
#include <seastar/core/file.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/units.hh>
#include <seastar/fs/filesystem.hh>
#include <seastar/fs/temporary_directory.hh>
#include <seastar/fs/temporary_file.hh>
#include <string>
#include <utility>
#include <vector>

using namespace seastar;
using namespace seastar::fs;

class kernel_fs_tester : public fs_tester {
    std::string _mount_point;

public:
    kernel_fs_tester(std::string mount_point, run_config rconf)
        : fs_tester(rconf), _mount_point(std::move(mount_point)) {}

    void create_files() override {
        auto create_files = [&](const std::string& prefix, size_t num) {
            std::vector<file_info> ret;
            for (size_t i = 0; i < num; ++i) {
                std::string filename = fmt::format("{}{}", prefix, i);
                file file = open_file_dma(filename, open_flags::rw | open_flags::create).get0();
                ret.push_back({std::move(file), 0});
            }
            return ret;
        };
        _small_files = create_files(fmt::format("{}/small_{}_", _mount_point, this_shard_id()), _rcfg.small_files_nb);
        _big_files = create_files(fmt::format("{}/big_{}_", _mount_point, this_shard_id()), _rcfg.big_files_nb);
    }
};

int main(int ac, char** av) {
    app_template at;
    namespace bpo = boost::program_options;
    at.add_options()
            ("small-files-nb", bpo::value<size_t>()->default_value(1),
                    "Number of small files (files with only small or medium writes)")
            ("big-files-nb", bpo::value<size_t>()->default_value(1), "Number of big files (files with only big writes)")
            ("write-prob", bpo::value<double>()->default_value(0.5),
                    "Total number of write operations divided by total number of all operations")
            ("small-prob", bpo::value<double>()->default_value(0.5),
                    "Total number of small file operations divided by total number of all operations")
            ("op-nb-limit", bpo::value<size_t>(), "Max number of operations, default: no limit")
            ("written-data-limit", bpo::value<std::string>(), "Max written data size, default: no limit")
            ("read-data-limit", bpo::value<std::string>(), "Max read data size, default: no limit")
            ("small-op-size-range", bpo::value<std::string>()->default_value("0,128k"), "Range of sizes for small operations")
            ("big-op-size-range", bpo::value<std::string>()->default_value("10M,20M"), "Range of sizes for big operations")
            ("seq-writes", bpo::value<bool>()->default_value(true), "Only sequential writes (at the end of files)")
            ("parallelism", bpo::value<size_t>()->default_value(64), "Reads and writes parallelism")
            ("runs-nb", bpo::value<size_t>()->default_value(100), "Number of runs")
            ("device-path", bpo::value<std::string>(), "Path to block device")
            ("fs-type", bpo::value<std::string>()->default_value("xfs"), "Filesystem type")
            ("name", bpo::value<std::string>()->default_value("simple test"), "Test name")
            ("no-stdout", "Do not print to stdout")
            ("json-output", bpo::value<std::string>(), "Output json file")
            ;
    return at.run(ac, av, [&at] {
        return async([&] {
            default_config fsconf;
            run_config rconf;
            rconf.aligned_ops = true;
            rconf.alignment = 4 * KB;
            rconf.small_files_nb = at.configuration()["small-files-nb"].as<size_t>();
            rconf.big_files_nb = at.configuration()["big-files-nb"].as<size_t>();
            assert(rconf.small_files_nb > 0 || rconf.big_files_nb > 0); // TODO: change to throws
            rconf.write_prob = at.configuration()["write-prob"].as<double>();
            assert(rconf.write_prob <= 1 && "Write prob should be in range [0, 1]");
            rconf.small_prob = at.configuration()["small-prob"].as<double>();
            assert(rconf.small_prob <= 1 && "Small file operations probability should be in range [0, 1]");
            assert((rconf.small_prob > 0 && rconf.small_files_nb > 0) || rconf.small_prob == 0);
            assert((rconf.small_prob < 1 && rconf.big_files_nb > 0) || rconf.small_prob == 1);
            rconf.op_nb_limit = at.configuration().count("op-nb-limit") == 0 ?
                    std::nullopt :
                    std::make_optional(at.configuration()["op-nb-limit"].as<size_t>());
            rconf.written_data_limit = at.configuration().count("written-data-limit") == 0 ?
                    std::nullopt :
                    std::make_optional(parse_memory_size(at.configuration()["written-data-limit"].as<std::string>()));
            rconf.read_data_limit = at.configuration().count("read-data-limit") == 0 ?
                    std::nullopt :
                    std::make_optional(parse_memory_size(at.configuration()["read-data-limit"].as<std::string>()));
            assert(rconf.op_nb_limit || rconf.written_data_limit || rconf.read_data_limit);
            rconf.parallelism = at.configuration()["parallelism"].as<size_t>();
            assert(rconf.parallelism > 0);
            rconf.small_op_size_range = parse_memory_range(at.configuration()["small-op-size-range"].as<std::string>());
            rconf.big_op_size_range = parse_memory_range(at.configuration()["big-op-size-range"].as<std::string>());
            rconf.seq_writes = at.configuration()["seq-writes"].as<bool>();

            fsconf.fs_type = parse_fs_type(at.configuration()["fs-type"].as<std::string>());

            std::unique_ptr<temporary_directory> td;
            std::unique_ptr<temporary_file> tf;
            if (at.configuration().count("device-path") == 0) {
                td = std::make_unique<temporary_directory>(".seastarfs_dir");
                tf = std::make_unique<temporary_file>(fmt::format("{}/seastarfs_file", td->path()));
                tf->truncate(400 * MB); // TODO: fix
                fsconf.device_path = tf->path();
            } else {
                // TODO: check device size
                fsconf.device_path = at.configuration()["device-path"].as<std::string>();
            }

            std::vector<std::unique_ptr<result_printer>> printers;
            if (!at.configuration().count("no-stdout")) {
                printers.emplace_back(std::make_unique<stdout_printer>());
            }
            if (at.configuration().count("json-output")) {
                printers.emplace_back(std::make_unique<json_printer>(
                    at.configuration()["json-output"].as<std::string>()
                ));
            }

            std::string test_name = at.configuration()["name"].as<std::string>();
            size_t runs_nb = at.configuration()["runs-nb"].as<size_t>();

            for (auto& rp : printers) {
                rp->print_configuration();
            }
            start_test<kernel_fs_tester>(test_name, runs_nb, fsconf, rconf, printers).get();
        });
    });
}
