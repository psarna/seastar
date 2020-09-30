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

#include <cstdint>
#include <functional>
#include <optional>
#include <random>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <utility>
#include <vector>

struct run_config {
    size_t small_files_nb;
    size_t big_files_nb;
    std::pair<size_t, size_t> big_op_size_range;
    std::pair<size_t, size_t> small_op_size_range;
    std::optional<size_t> op_nb_limit;
    std::optional<size_t> written_data_limit;
    std::optional<size_t> read_data_limit;
    double write_prob;
    double small_prob;
    double small_write_prob;
    size_t alignment;
    size_t parallelism;
    bool aligned_ops;
    bool seq_writes;
};

class fs_tester {
protected:
    struct run_execution_config {
        std::optional<size_t> op_nb_limit;
        std::optional<size_t> written_data_limit;
        std::optional<size_t> read_data_limit;
        size_t parallelism;
        double write_prob;
    } _recfg;

    struct data_gen_config {
        size_t small_files_nb;
        size_t big_files_nb;
        std::pair<size_t, size_t> big_op_size_range;
        std::pair<size_t, size_t> small_op_size_range;
        double small_prob;
        double small_write_prob;
        size_t alignment;
        bool aligned_ops;
        bool seq_writes;
    } _dgcfg;

    std::default_random_engine _random_engine;

    struct file_info {
        seastar::file _file;
        size_t _size;
    };

    std::vector<file_info> _small_files;
    std::vector<file_info> _big_files;

    size_t total_files_size = 0;

    std::uniform_real_distribution<double> _prob_dist;
    std::uniform_int_distribution<size_t> _small_files_dist;
    std::uniform_int_distribution<size_t> _big_files_dist;
    std::function<size_t()> _gen_small_op_size;
    std::function<size_t()> _gen_big_op_size;

    seastar::temporary_buffer<uint8_t> _base_buffer;

    seastar::temporary_buffer<uint8_t> share_base_buffer(size_t size);

    seastar::temporary_buffer<uint8_t> gen_small_buffer();
    seastar::temporary_buffer<uint8_t> gen_big_buffer();

    seastar::temporary_buffer<uint8_t> allocate_read_buffer(size_t size);

    size_t gen_small_op_size();
    size_t gen_big_op_size();

    size_t gen_op_pos(size_t min, size_t max);

public:
    fs_tester(run_config rconf);

protected:
    seastar::future<> do_truncate();
    // total_write_len is incremented before write completes
    seastar::future<> do_write(size_t& total_write_len);
    // total_read_len is incremented before write completes
    seastar::future<> do_read(size_t& total_read_len);

    virtual seastar::future<> post_test_callback() { return seastar::now(); }

    seastar::future<> run_execution(run_execution_config& recfg);

public:
    seastar::future<> run();

protected:
    virtual size_t filesystem_size() const noexcept = 0;

    void setup_generators();
    // Create files that will be used for testing by run() in do_write()/do_read()
    virtual void create_files() = 0;
    // Implementation should prepare initial state of the filesystem (files) before starting run().
    // setup_fs_state() is used to test more realistic scenarios where filesystem isn't brand new but
    // was used before.
    virtual void setup_fs_state();

public:
    seastar::future<> init();
    seastar::future<> stop();
};
