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

class sfs_tester : public fs_tester {
    filesystem& _fs;
    const size_t _filesystem_size;

public:
    sfs_tester(filesystem& fs, run_config rconf)
        : fs_tester(rconf), _fs(fs), _filesystem_size(_fs.remaining_space()) {}

    future<> post_test_callback() override {
        return _fs.flush();
    }

protected:
    size_t filesystem_size() const noexcept override {
        return _filesystem_size;
    }

    void create_files() override {
        auto create_files = [&](const std::string& prefix, size_t num) {
            std::vector<file_info> ret;
            for (size_t i = 0; i < num; ++i) {
                std::string filename = fmt::format("{}{}", prefix, i);
                file file = _fs.create_and_open_file(filename, open_flags::rw).get0();
                ret.push_back({std::move(file), 0});
            }
            return ret;
        };

        std::string shard_dir = fmt::format("/{}", this_shard_id());
        _fs.create_directory(shard_dir).get();
        _small_files = create_files(fmt::format("{}/small_", shard_dir), _rcfg.small_files_nb);
        _big_files = create_files(fmt::format("{}/big_", shard_dir), _rcfg.big_files_nb);
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
                    "Total number of small truncates divided by total number of all truncates")
            ("small-write-prob", bpo::value<double>()->default_value(0.5),
                    "Total number of small writes divided by total number of all writes")
            ("op-nb-limit", bpo::value<size_t>(), "Max number of operations, default: no limit")
            ("written-data-limit", bpo::value<std::string>(), "Max written data size, default: no limit")
            ("read-data-limit", bpo::value<std::string>(), "Max read data size, default: no limit")
            ("alignment", bpo::value<std::string>()->default_value("4k"), "Alignment for reads and writes used by filesystem")
            ("cluster-size", bpo::value<std::string>()->default_value("1M"), "Size of clusters in filesystem")
            ("compactness", bpo::value<double>()->default_value(0), "Compactness used by compaction")
            ("compaction-max-memory-size", bpo::value<std::string>()->default_value("0"), "Max memory used for compaction")
            ("small-op-size-range", bpo::value<std::string>()->default_value("0,128k"), "Range of sizes for small operations")
            ("big-op-size-range", bpo::value<std::string>()->default_value("10M,20M"), "Range of sizes for big operations")
            ("seq-writes", bpo::value<bool>()->default_value(true), "Only sequential writes (at the end of files)")
            ("aligned-ops", bpo::value<bool>()->default_value(true), "Align writes and reads")
            ("parallelism", bpo::value<size_t>()->default_value(64), "Reads and writes parallelism")
            ("runs-nb", bpo::value<size_t>()->default_value(100), "Number of runs")
            ("device-path", bpo::value<std::string>(), "Path to block device")
            ("name", bpo::value<std::string>()->default_value("simple test"), "Test name")
            ("no-stdout", "Do not print to stdout")
            ("json-output", bpo::value<std::string>(), "Output json file")
            ;
    return at.run(ac, av, [&at] {
        return async([&] {
            sfs_config fsconf;
            run_config rconf;
            rconf.small_files_nb = at.configuration()["small-files-nb"].as<size_t>();
            rconf.big_files_nb = at.configuration()["big-files-nb"].as<size_t>();
            assert(rconf.small_files_nb > 0 || rconf.big_files_nb > 0); // TODO: change to throws
            rconf.write_prob = at.configuration()["write-prob"].as<double>();
            assert(rconf.write_prob <= 1 && "Write prob should be in range [0, 1]");
            rconf.small_prob = at.configuration()["small-prob"].as<double>();
            assert(rconf.small_prob <= 1 && "Small read/truncate probability should be in range [0, 1]");
            assert((rconf.small_prob > 0 && rconf.small_files_nb > 0) || rconf.small_prob == 0);
            assert((rconf.small_prob < 1 && rconf.big_files_nb > 0) || rconf.small_prob == 1);
            rconf.small_write_prob = at.configuration()["small-write-prob"].as<double>();
            assert(rconf.small_write_prob <= 1 && "Small write probability should be in range [0, 1]");
            assert((rconf.small_write_prob > 0 && rconf.small_files_nb > 0) || rconf.small_write_prob == 0);
            assert((rconf.small_write_prob < 1 && rconf.big_files_nb > 0) || rconf.small_write_prob == 1);
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
            rconf.alignment = parse_memory_size(at.configuration()["alignment"].as<std::string>());
            fsconf.alignment = rconf.alignment;
            fsconf.cluster_size = parse_memory_size(at.configuration()["cluster-size"].as<std::string>());
            fsconf.compactness = at.configuration()["compactness"].as<double>();
            fsconf.compaction_max_memory_size = parse_memory_size(at.configuration()["cluster-size"].as<std::string>());
            rconf.aligned_ops = at.configuration()["aligned-ops"].as<bool>();
            rconf.parallelism = at.configuration()["parallelism"].as<size_t>();
            assert(rconf.parallelism > 0);
            rconf.small_op_size_range = parse_memory_range(at.configuration()["small-op-size-range"].as<std::string>());
            rconf.big_op_size_range = parse_memory_range(at.configuration()["big-op-size-range"].as<std::string>());
            rconf.seq_writes = at.configuration()["seq-writes"].as<bool>();

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
            start_test<sfs_tester>(test_name, runs_nb, fsconf, rconf, printers).get();
        });
    });
}
