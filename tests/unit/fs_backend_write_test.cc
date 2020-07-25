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
#include "fs/backend/write.hh"
#include "fs/cluster_utils.hh"
#include "fs_backend_cluster_writer_mocker.hh"
#include "fs_backend_metadata_log_to_disk_buffer_mocker.hh"
#include "fs_backend_shard_tester.hh"
#include "fs_random.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/fs/bitwise.hh"
#include "seastar/fs/unit_types.hh"
#include "seastar/testing/thread_test_case.hh"

using std::vector;

using namespace seastar;
using namespace seastar::fs;
namespace ml = seastar::fs::backend::metadata_log;
namespace mle = seastar::fs::backend::metadata_log::entries;

template<class T>
std::pair<T, T> sorted(T a, T b) {
    if (a < b) {
        return {a, b};
    }
    return {b, a};
}

using resizable_buff_type = basic_sstring<uint8_t, size_t, 32, false>;

struct write_test_base {
    backend::shard_tester st;
    inode_t inode = st.create_and_open_file();
    ml::to_disk_buffer_mocker& ml_buff = st.curr_ml_buff();
    resizable_buff_type real_file_data;

    write_test_base() = default;
    write_test_base(const struct backend::shard_tester::options& options) : st(options) {};

    static constexpr auto SMALL_WRITE_THRESHOLD = backend::write_operation::SMALL_WRITE_THRESHOLD;
    const size_t min_medium_write_len =
        round_up_to_multiple_of_power_of_2(SMALL_WRITE_THRESHOLD + 1, st.options.alignment);
    static constexpr size_t random_read_checks_nb = 100;

    temporary_buffer<uint8_t> gen_buffer(size_t len, bool aligned) {
        temporary_buffer<uint8_t> buff;
        if (!aligned) {
            // Make buff unaligned
            buff = temporary_buffer<uint8_t>::aligned(st.options.alignment,
                    round_up_to_multiple_of_power_of_2(len, st.options.alignment) + st.options.alignment);
            size_t offset = random_value<decltype(st.options.alignment)>(1, st.options.alignment - 1);
            buff.trim_front(offset);
        } else {
            buff = temporary_buffer<uint8_t>::aligned(st.options.alignment,
                    round_up_to_multiple_of_power_of_2(len, st.options.alignment));
        }
        buff.trim(len);
        random_overwrite(buff.get_write(), buff.size());
        return buff;
    }

    enum class write_type {
        SMALL,
        MEDIUM,
        LARGE
    };

    write_type get_write_type(size_t len) noexcept {
        return len <= SMALL_WRITE_THRESHOLD ? write_type::SMALL :
                (len >= st.options.cluster_size ? write_type::LARGE : write_type::MEDIUM);
    }

    void write_with_simulate(file_offset_t write_offset, temporary_buffer<uint8_t>& buff) {
        if (real_file_data.size() < write_offset + buff.size()) {
            real_file_data.resize(write_offset + buff.size());
        }
        std::memcpy(real_file_data.data() + write_offset, buff.get(), buff.size());

        BOOST_REQUIRE_EQUAL(st.shard.write(inode, write_offset, buff.get(), buff.size()).get0(), buff.size());
    }

    void random_write_with_simulate(file_offset_t write_offset, size_t bytes_num, bool aligned) {
        temporary_buffer<uint8_t> buff = gen_buffer(bytes_num, aligned);
        write_with_simulate(write_offset, buff);
    }

    void check_random_reads(size_t reps = random_read_checks_nb, bool must_be_aligned = false) {
        size_t file_size = real_file_data.size();
        auto gen_value = [&](file_offset_t min, file_offset_t max) {
            if (!must_be_aligned) {
                return random_value(min, max);
            }

            auto aligned_min = round_up_to_multiple_of_power_of_2(min, st.options.alignment);
            auto aligned_max = round_down_to_multiple_of_power_of_2(max, st.options.alignment);
            assert(aligned_min < aligned_max);
            return random_value(aligned_min / st.options.alignment, aligned_max / st.options.alignment) * st.options.alignment;
        };

        {
            // Check random reads inside the file
            for (size_t rep = 0; rep < reps; ++rep) {
                auto [a, b] = sorted(gen_value(0, file_size), gen_value(0, file_size));
                size_t max_read_size = b - a;
                temporary_buffer<uint8_t> read_data(max_read_size);
                BOOST_REQUIRE_EQUAL(st.shard.read(inode, a, read_data.get_write(), max_read_size).get0(), max_read_size);
                BOOST_REQUIRE(std::memcmp(real_file_data.c_str() + a, read_data.get(), max_read_size) == 0);
            }
        }

        {
            // Check random reads outside the file
            for (size_t rep = 0; rep < reps; ++rep) {
                auto [a, b] = sorted(gen_value(file_size, 2 * file_size), gen_value(file_size, 2 * file_size));
                size_t max_read_size = b - a;
                temporary_buffer<uint8_t> read_data(max_read_size);
                BOOST_REQUIRE_EQUAL(st.shard.read(inode, a, read_data.get_write(), max_read_size).get0(), 0);
            }
        }

        {
            // Check random reads on the edge of the file
            for (size_t rep = 0; rep < reps; ++rep) {
                auto a = gen_value(0, file_size);
                auto b = gen_value(file_size, 2 * file_size);
                size_t max_read_size = b - a;
                size_t expected_read_size = file_size - a;
                temporary_buffer<uint8_t> read_data(max_read_size);
                BOOST_REQUIRE_EQUAL(st.shard.read(inode, a, read_data.get_write(), max_read_size).get0(), expected_read_size);
                BOOST_REQUIRE(std::memcmp(real_file_data.c_str() + a, read_data.get(), expected_read_size) == 0);
            }
        }
    }
};

#define CHECK_CALL(...) BOOST_TEST_CONTEXT("Called from line: " << __LINE__) { __VA_ARGS__; }

BOOST_TEST_DONT_PRINT_LOG_VALUE(temporary_buffer<uint8_t>)

SEASTAR_THREAD_TEST_CASE(small_write_test) {
    struct tester : write_test_base {
        void body(size_t write_len) {
            BOOST_TEST_MESSAGE("write_len: " << write_len);
            constexpr auto write_offset = 7312;
            assert(get_write_type(write_len) == write_type::SMALL);

            auto buff = gen_buffer(write_len, false);
            const auto time_ns = st.clock.refreeze_time_ns();
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);

            // Check metadata
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 2);
            BOOST_REQUIRE_EQUAL(ml_buff.actions.back(), (mle::small_write{
                .inode = inode,
                .offset = write_offset,
                .time_ns = time_ns,
                .data = buff.share(),
            }));
            CHECK_CALL(check_random_reads());
        }

        void test1() { body(1); }
        void test2() { body(10); }
        void test3() { body(SMALL_WRITE_THRESHOLD); }
    };

    tester().test1();
    tester().test2();
    tester().test3();
}

SEASTAR_THREAD_TEST_CASE(medium_write_test) {
    struct tester : write_test_base {
        void body(size_t write_len) {
            BOOST_TEST_MESSAGE("write_len: " << write_len);
            assert(write_len % st.options.alignment == 0);
            assert(get_write_type(write_len) == write_type::MEDIUM);

            const auto time_ns = st.clock.refreeze_time_ns();
            auto buff = gen_buffer(write_len, true);
            constexpr auto write_offset = 7331;
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);

            // Check data
            auto& clst_writer_ops = st.curr_c_writer().writes;
            BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 1);
            BOOST_REQUIRE_EQUAL(clst_writer_ops[0].data, buff);
            // Check metadata
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 2);
            BOOST_REQUIRE_EQUAL(ml_buff.actions.back(), (mle::medium_write{
                .inode = inode,
                .offset = write_offset,
                .drange = {
                    .beg = clst_writer_ops[0].disk_offset,
                    .end = clst_writer_ops[0].disk_offset + write_len,
                },
                .time_ns = time_ns,
            }));
            CHECK_CALL(check_random_reads(inode));
        }

        void test1() { body(min_medium_write_len); }
        void test2() { body(st.options.cluster_size - st.options.alignment); }
        void test3() { body(round_up_to_multiple_of_power_of_2(st.options.cluster_size / 3 + 10, st.options.alignment)); }
    };

    tester().test1();
    tester().test2();
    tester().test3();
}

SEASTAR_THREAD_TEST_CASE(second_medium_write_without_new_data_cluster_allocation_test) {
    struct tester : write_test_base {
        const size_t second_write_len =
                round_up_to_multiple_of_power_of_2(2 * SMALL_WRITE_THRESHOLD + 1, st.options.alignment);
        void body(size_t first_write_len) {
            BOOST_TEST_MESSAGE("first write len: " << first_write_len);
            BOOST_TEST_MESSAGE("second write len: " << second_write_len);
            constexpr auto write_offset = 1337;
            assert(first_write_len % st.options.alignment == 0);
            assert(get_write_type(first_write_len) == write_type::MEDIUM);
            assert(get_write_type(second_write_len) == write_type::MEDIUM);
            auto remaining_space_in_cluster = st.options.cluster_size - first_write_len;
            assert(remaining_space_in_cluster >= SMALL_WRITE_THRESHOLD);
            assert(remaining_space_in_cluster >= second_write_len);

            // After that write remaining_space_in_cluster bytes should remain in internal cluster_writer
            CHECK_CALL(random_write_with_simulate(0, first_write_len, true));
            size_t nb_of_cluster_writers_before = st.c_writers.size();
            temporary_buffer<uint8_t> buff = gen_buffer(second_write_len, true);
            const auto time_ns = st.clock.refreeze_time_ns();
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);
            BOOST_REQUIRE_EQUAL(nb_of_cluster_writers_before, st.c_writers.size());

            // Check cluster writer data
            auto& clst_writer_ops = st.curr_c_writer().writes;
            BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 2);
            BOOST_REQUIRE_EQUAL(clst_writer_ops[1], (backend::cluster_writer_mocker::write_to_device{
                .disk_offset = clst_writer_ops[0].disk_offset + first_write_len,
                .data = buff.share(),
            }));
            // Check metadata to disk buffer entries
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 3);
            BOOST_REQUIRE_EQUAL(ml_buff.actions[2], (mle::medium_write{
                .inode = inode,
                .offset = write_offset,
                .drange = {
                    .beg = clst_writer_ops[1].disk_offset,
                    .end = clst_writer_ops[1].disk_offset + second_write_len,
                },
                .time_ns = time_ns,
            }));
            CHECK_CALL(check_random_reads());
        }

        void test1() { body(st.options.cluster_size - second_write_len - st.options.alignment); }
        void test2() { body(st.options.cluster_size - second_write_len); }
    };

    tester().test1();
    tester().test2();
}

SEASTAR_THREAD_TEST_CASE(second_medium_write_with_new_data_cluster_allocation_test) {
    struct tester : write_test_base {
        const size_t second_write_len = min_medium_write_len;
        void body(size_t first_write_len) {
            BOOST_TEST_MESSAGE("first write len: " << first_write_len);
            BOOST_TEST_MESSAGE("second write len: " << second_write_len);
            constexpr auto write_offset = 1337;
            assert(first_write_len % st.options.alignment == 0);
            assert(get_write_type(first_write_len) == write_type::MEDIUM);
            assert(get_write_type(second_write_len) == write_type::MEDIUM);
            auto remaining_space_in_cluster = st.options.cluster_size - first_write_len;
            assert(remaining_space_in_cluster <= SMALL_WRITE_THRESHOLD);

            // After that write remaining_space_in_cluster bytes should remain in internal cluster_writer
            CHECK_CALL(random_write_with_simulate(0, first_write_len, true));
            const auto time_ns = st.clock.refreeze_time_ns();
            size_t nb_of_cluster_writers_before = st.c_writers.size();
            temporary_buffer<uint8_t> buff = gen_buffer(second_write_len, true);
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);
            BOOST_REQUIRE_EQUAL(nb_of_cluster_writers_before + 1, st.c_writers.size());

            // Check data
            auto& clst_writer_ops = st.curr_c_writer().writes;
            BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 1);
            BOOST_REQUIRE_EQUAL(clst_writer_ops.back().data.size(), buff.size());
            BOOST_REQUIRE_EQUAL(clst_writer_ops.back().data, buff);
            // Check metadata
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 3);
            BOOST_REQUIRE_EQUAL(ml_buff.actions[2], (mle::medium_write{
                .inode = inode,
                .offset = write_offset,
                .drange = {
                    .beg = clst_writer_ops[0].disk_offset,
                    .end = clst_writer_ops[0].disk_offset + second_write_len,
                },
                .time_ns = time_ns,
            }));
            CHECK_CALL(check_random_reads());
        }

        void test1() { body(st.options.cluster_size - st.options.alignment); }
        void test2() {
            body(st.options.cluster_size - round_down_to_multiple_of_power_of_2(SMALL_WRITE_THRESHOLD, st.options.alignment));
        }
    };

    tester().test1();
    tester().test2();
}

SEASTAR_THREAD_TEST_CASE(split_medium_write_with_small_write_test) {
    struct tester : write_test_base {
        void body(size_t first_write_len, size_t second_write_len) {
            BOOST_TEST_MESSAGE("first write len: " << first_write_len);
            BOOST_TEST_MESSAGE("second write len: " << second_write_len);
            constexpr auto write_offset = 1337;
            assert(first_write_len % st.options.alignment == 0);
            assert(get_write_type(first_write_len) == write_type::MEDIUM);
            assert(get_write_type(second_write_len) == write_type::MEDIUM);
            auto remaining_space_in_cluster = st.options.cluster_size - first_write_len;
            assert(remaining_space_in_cluster > SMALL_WRITE_THRESHOLD);
            assert(remaining_space_in_cluster < second_write_len);
            assert(get_write_type(second_write_len - remaining_space_in_cluster) == write_type::SMALL);

            // After that write remaining_space_in_cluster bytes should remain in internal cluster_writer
            CHECK_CALL(random_write_with_simulate(0, first_write_len, true));
            const auto time_ns = st.clock.refreeze_time_ns();
            size_t nb_of_cluster_writers_before = st.c_writers.size();
            temporary_buffer<uint8_t> buff = gen_buffer(second_write_len, true);
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);
            BOOST_REQUIRE_EQUAL(nb_of_cluster_writers_before, st.c_writers.size());

            // Check data
            auto& clst_writer_ops = st.curr_c_writer().writes;
            BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 2);
            BOOST_REQUIRE_EQUAL(clst_writer_ops.back().data, buff.share(0, remaining_space_in_cluster));
            // Check metadata
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 4);
            BOOST_REQUIRE_EQUAL(ml_buff.actions[2], (mle::medium_write{
                .inode = inode,
                .offset = write_offset,
                .drange = {
                    .beg = clst_writer_ops[1].disk_offset,
                    .end = clst_writer_ops[1].disk_offset + remaining_space_in_cluster,
                },
                .time_ns = time_ns,
            }));
            BOOST_REQUIRE_EQUAL(ml_buff.actions[3], (mle::small_write{
                .inode = inode,
                .offset = write_offset + remaining_space_in_cluster,
                .time_ns = time_ns,
                .data = buff.share(remaining_space_in_cluster, second_write_len - remaining_space_in_cluster),
            }));
            CHECK_CALL(check_random_reads());
        }

        void test1() {
            body(st.options.cluster_size - min_medium_write_len, min_medium_write_len + SMALL_WRITE_THRESHOLD);
        }
        void test2() {
            body(st.options.cluster_size - 3 * min_medium_write_len, 3 * min_medium_write_len + 1);
        }
    };

    tester().test1();
    tester().test2();
}

SEASTAR_THREAD_TEST_CASE(split_medium_write_with_medium_write_test) {
    struct tester : write_test_base {
        void body(size_t first_write_len, size_t second_write_len) {
            BOOST_TEST_MESSAGE("first write len: " << first_write_len);
            BOOST_TEST_MESSAGE("second write len: " << second_write_len);
            constexpr auto write_offset = 1337;
            assert(first_write_len % st.options.alignment == 0);
            assert(get_write_type(first_write_len) == write_type::MEDIUM);
            assert(get_write_type(second_write_len) == write_type::MEDIUM);
            auto remaining_space_in_cluster = st.options.cluster_size - first_write_len;
            assert(remaining_space_in_cluster > SMALL_WRITE_THRESHOLD);
            assert(remaining_space_in_cluster < second_write_len);
            assert(get_write_type(second_write_len - remaining_space_in_cluster) == write_type::MEDIUM);

            // After that write remaining_space_in_cluster bytes should remain in internal cluster_writer
            CHECK_CALL(random_write_with_simulate(0, first_write_len, true));
            const auto time_ns = st.clock.refreeze_time_ns();
            size_t nb_of_cluster_writers_before = st.c_writers.size();
            temporary_buffer<uint8_t> buff = gen_buffer(second_write_len, true);
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);
            BOOST_REQUIRE_EQUAL(nb_of_cluster_writers_before + 1, st.c_writers.size());

            // Check data
            auto& prev_clst_writer = st.c_writers[st.c_writers.size() - 2];
            auto& prev_clst_writer_ops = prev_clst_writer->writes;
            BOOST_REQUIRE_EQUAL(prev_clst_writer_ops.size(), 2);
            BOOST_REQUIRE_EQUAL(prev_clst_writer_ops.back().data, buff.share(0, remaining_space_in_cluster));
            auto& clst_writer_ops = st.curr_c_writer().writes;
            BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 1);
            BOOST_REQUIRE_EQUAL(clst_writer_ops.back().data,
                    buff.share(remaining_space_in_cluster, second_write_len - remaining_space_in_cluster));
            // Check metadata
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 4);
            BOOST_REQUIRE_EQUAL(ml_buff.actions[2], (mle::medium_write{
                .inode = inode,
                .offset = write_offset,
                .drange = {
                    .beg = prev_clst_writer_ops[1].disk_offset,
                    .end = prev_clst_writer_ops[1].disk_offset + remaining_space_in_cluster,
                },
                .time_ns = time_ns,
            }));
            BOOST_REQUIRE_EQUAL(ml_buff.actions[3], (mle::medium_write{
                .inode = inode,
                .offset = write_offset + remaining_space_in_cluster,
                .drange = {
                    .beg = clst_writer_ops[0].disk_offset,
                    .end = clst_writer_ops[0].disk_offset + second_write_len - remaining_space_in_cluster,
                },
                .time_ns = time_ns,
            }));
            CHECK_CALL(check_random_reads());
        }

        void test1() {
            body(st.options.cluster_size - min_medium_write_len, 2 * min_medium_write_len);
        }
        void test2() {
            body(st.options.cluster_size - min_medium_write_len, st.options.cluster_size - st.options.alignment);
        }
    };

    tester().test1();
    tester().test2();
}

SEASTAR_THREAD_TEST_CASE(large_write_test) {
    struct tester : write_test_base {
        void test() {
            const auto write_len = st.options.cluster_size * 2;
            BOOST_TEST_MESSAGE("write_len: " << write_len);
            constexpr auto write_offset = 7331;
            // TODO: asserts

            const auto time_ns = st.clock.refreeze_time_ns();
            temporary_buffer<uint8_t> buff = gen_buffer(write_len, true);
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);

            // Check data
            auto& blockdev_ops = st.device.writes;
            BOOST_REQUIRE_EQUAL(blockdev_ops.size(), 2);
            BOOST_REQUIRE_EQUAL(blockdev_ops[0].disk_offset % st.options.cluster_size, 0);
            BOOST_REQUIRE_EQUAL(blockdev_ops[1].disk_offset % st.options.cluster_size, 0);
            auto part1_cluster_id = offset_to_cluster_id(blockdev_ops[0].disk_offset, st.options.cluster_size);
            auto part2_cluster_id = offset_to_cluster_id(blockdev_ops[1].disk_offset, st.options.cluster_size);
            BOOST_REQUIRE_EQUAL(blockdev_ops[0].data, buff.share(0, st.options.cluster_size));
            BOOST_REQUIRE_EQUAL(blockdev_ops[1].data, buff.share(st.options.cluster_size, st.options.cluster_size));
            // Check metadata
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 3);
            BOOST_REQUIRE_EQUAL(ml_buff.actions[1], (mle::large_write{
                .lwwt = {
                    .inode = inode,
                    .offset = write_offset,
                    .data_cluster = part1_cluster_id,
                },
                .time_ns = time_ns,
            }));
            BOOST_REQUIRE_EQUAL(ml_buff.actions[2], (mle::large_write_without_time{
                .inode = inode,
                .offset = write_offset + st.options.cluster_size,
                .data_cluster = part2_cluster_id,
            }));
            CHECK_CALL(check_random_reads());
        }
    };

    tester().test();
}

SEASTAR_THREAD_TEST_CASE(unaligned_write_split_into_two_small_writes_test) {
    struct tester : write_test_base {
        void test() {
            // medium write split into two small writes
            constexpr auto write_len = SMALL_WRITE_THRESHOLD + 1;
            BOOST_TEST_MESSAGE("write_len: " << write_len);
            constexpr auto write_offset = 7331;
            // TODO: asserts

            const auto time_ns = st.clock.refreeze_time_ns();
            temporary_buffer<uint8_t> buff = gen_buffer(write_len, false);
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);

            auto misalignment = reinterpret_cast<uintptr_t>(buff.get()) % st.options.alignment;
            auto part1_write_len = st.options.alignment - misalignment;
            auto part2_write_len = write_len - part1_write_len;

            // Check metadata
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 3);
            BOOST_REQUIRE_EQUAL(ml_buff.actions[1], (mle::small_write{
                .inode = inode,
                .offset = write_offset,
                .time_ns = time_ns,
                .data = buff.share(0, part1_write_len),
            }));
            BOOST_REQUIRE_EQUAL(ml_buff.actions[2], (mle::small_write{
                .inode = inode,
                .offset = write_offset + part1_write_len,
                .time_ns = time_ns,
                .data = buff.share(part1_write_len, part2_write_len),
            }));
            CHECK_CALL(check_random_reads());
        }
    };

    tester().test();
}

SEASTAR_THREAD_TEST_CASE(unaligned_write_split_into_small_medium_and_small_writes_test) {
    struct tester : write_test_base {
        void body(size_t write_len) {
            BOOST_TEST_MESSAGE("write_len: " << write_len);
            constexpr auto write_offset = 7331;
            // TODO: asserts

            const auto time_ns = st.clock.refreeze_time_ns();
            temporary_buffer<uint8_t> buff = gen_buffer(write_len, false);
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);

            auto misalignment = reinterpret_cast<uintptr_t>(buff.get()) % st.options.alignment;
            auto part1_write_len = st.options.alignment - misalignment;
            auto part2_write_len = round_down_to_multiple_of_power_of_2(write_len - part1_write_len, st.options.alignment);
            auto part3_write_len = write_len - part1_write_len - part2_write_len;

            // Check data
            auto& clst_writer_ops = st.curr_c_writer().writes;
            BOOST_REQUIRE_EQUAL(clst_writer_ops.size(), 1);
            BOOST_REQUIRE_EQUAL(clst_writer_ops.back().data, buff.share(part1_write_len, part2_write_len));
            // Check metadata
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 4);
            BOOST_REQUIRE_EQUAL(ml_buff.actions[1], (mle::small_write{
                .inode = inode,
                .offset = write_offset,
                .time_ns = time_ns,
                .data = buff.share(0, part1_write_len),
            }));
            BOOST_REQUIRE_EQUAL(ml_buff.actions[2], (mle::medium_write{
                .inode = inode,
                .offset = write_offset + part1_write_len,
                .drange = {
                    .beg = clst_writer_ops.back().disk_offset,
                    .end = clst_writer_ops.back().disk_offset + part2_write_len,
                },
                .time_ns = time_ns,
            }));
            BOOST_REQUIRE_EQUAL(ml_buff.actions[3], (mle::small_write{
                .inode = inode,
                .offset = write_offset + part1_write_len + part2_write_len,
                .time_ns = time_ns,
                .data = buff.share(part1_write_len + part2_write_len, part3_write_len),
            }));
            CHECK_CALL(check_random_reads());
        }

        void test1() { body(st.options.cluster_size - st.options.alignment); }
        void test2() { body(st.options.cluster_size); }
    };

    tester().test1();
    tester().test2();
}

SEASTAR_THREAD_TEST_CASE(unaligned_write_split_into_small_large_and_small_writes_test) {
    struct tester : write_test_base {
        void test() {
            const auto write_len = st.options.cluster_size + st.options.alignment;
            BOOST_TEST_MESSAGE("write_len: " << write_len);
            constexpr auto write_offset = 7331;
            // TODO: asserts

            const auto time_ns = st.clock.refreeze_time_ns();
            temporary_buffer<uint8_t> buff = gen_buffer(write_len, false);
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);

            auto misalignment = reinterpret_cast<uintptr_t>(buff.get()) % st.options.alignment;
            auto part1_write_len = st.options.alignment - misalignment;
            auto part3_write_len = write_len - part1_write_len - st.options.cluster_size;

            // Check data
            auto& blockdev_ops = st.device.writes;
            BOOST_REQUIRE_EQUAL(blockdev_ops.size(), 1);
            BOOST_REQUIRE_EQUAL(blockdev_ops.back().disk_offset % st.options.cluster_size, 0);
            auto part2_cluster_id = offset_to_cluster_id(blockdev_ops.back().disk_offset, st.options.cluster_size);
            BOOST_REQUIRE_EQUAL(blockdev_ops.back().data, buff.share(part1_write_len, st.options.cluster_size));
            // Check metadata
            BOOST_REQUIRE_EQUAL(ml_buff.actions.size(), 4);
            BOOST_REQUIRE_EQUAL(ml_buff.actions[1], (mle::small_write{
                .inode = inode,
                .offset = write_offset,
                .time_ns = time_ns,
                .data = buff.share(0, part1_write_len),
            }));
            BOOST_REQUIRE_EQUAL(ml_buff.actions[2], (mle::large_write_without_time{
                .inode = inode,
                .offset = write_offset + part1_write_len,
                .data_cluster = part2_cluster_id,
            }));
            BOOST_REQUIRE_EQUAL(ml_buff.actions[3], (mle::small_write{
                .inode = inode,
                .offset = write_offset + part1_write_len + st.options.cluster_size,
                .time_ns = time_ns,
                .data = buff.share(part1_write_len + st.options.cluster_size, part3_write_len),
            }));
            CHECK_CALL(check_random_reads());
        }
    };

    tester().test();
}

SEASTAR_THREAD_TEST_CASE(big_single_write_splitting_test) {
    struct tester : write_test_base {
        void test() {
            const uint64_t write_len = st.options.cluster_size * 3 + min_medium_write_len + st.options.alignment + 10;
            BOOST_TEST_MESSAGE("write_len: " << write_len);
            constexpr auto write_offset = 7331;

            temporary_buffer<uint8_t> buff = gen_buffer(write_len, true);
            CHECK_CALL(write_with_simulate(write_offset, buff));
            BOOST_TEST_MESSAGE("ml_buff.actions: " << ml_buff.actions);

            auto& meta_actions = ml_buff.actions;
            BOOST_REQUIRE_EQUAL(meta_actions.size(), 6);
            BOOST_REQUIRE(std::holds_alternative<mle::large_write>(meta_actions[1]));
            BOOST_REQUIRE(std::holds_alternative<mle::large_write_without_time>(meta_actions[2]));
            BOOST_REQUIRE(std::holds_alternative<mle::large_write_without_time>(meta_actions[3]));
            BOOST_REQUIRE(std::holds_alternative<mle::medium_write>(meta_actions[4]));
            BOOST_REQUIRE(std::holds_alternative<mle::small_write>(meta_actions[5]));
            CHECK_CALL(check_random_reads());
        }
    };

    tester().test();
}

SEASTAR_THREAD_TEST_CASE(random_writes_and_reads_test) {
    constexpr auto cluster_size = 128 * KB;
    constexpr auto random_read_checks_nb_every_write = 30;
    static constexpr auto writes_nb = 300;
    constexpr auto max_file_size = cluster_size * 3;

    struct tester : write_test_base {
        tester() : write_test_base((struct backend::shard_tester::options){
            .cluster_size = cluster_size,
            .available_clusters = {
                .beg = 1,
                .end = offset_to_cluster_id(max_file_size * writes_nb * 2, cluster_size) + 1,
            }
        }) {};

        void body(bool aligned_offsets) {
            BOOST_TEST_MESSAGE("available clusters: " << st.options.available_clusters.size());
            auto random_offset = [&] {
                if (!aligned_offsets) {
                    return random_value(0, max_file_size - 1);
                }
                assert(max_file_size % st.options.alignment == 0);
                return random_value(0, max_file_size / st.options.alignment) * st.options.alignment;
            };
            for (size_t rep = 1; rep <= writes_nb; ++rep) {
                if (rep % (writes_nb / 10) == 0) {
                    BOOST_TEST_MESSAGE("rep: " << rep << "/" << writes_nb);
                }
                auto [a, b] = sorted(random_offset(), random_offset());
                size_t write_size = b - a + 1;
                bool aligned = aligned_offsets || random_value(0, 1);
                CHECK_CALL(random_write_with_simulate(a, write_size, aligned));
                CHECK_CALL(check_random_reads(random_read_checks_nb_every_write, aligned_offsets));
            }
        }

        void test_all() { body(false); }
        void test_aligned() { body(true); }
    };

    tester().test_all();
    tester().test_aligned();
}
