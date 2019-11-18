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
 * Copyright (C) 2019 ScyllaDB
 */

#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/file-types.hh"
#include "seastar/core/file.hh"
#include "seastar/core/thread.hh"
#include "seastar/core/units.hh"
#include "seastar/fs/file.hh"
#include "seastar/fs/temporary_file.hh"
#include "seastar/testing/thread_test_case.hh"

using namespace seastar;
using namespace fs;

constexpr auto device_path = "/tmp/seastarfs";
constexpr auto device_size = 16 * MB;

SEASTAR_THREAD_TEST_CASE(parallel_read_write_test) {
    const auto tf = temporary_file(device_path);
    auto f = fs::open_file_dma(tf.path(), open_flags::rw).get0();
    static auto alignment = f.memory_dma_alignment();

    parallel_for_each(boost::irange<off_t>(0, device_size / alignment), [&f](auto i) {
        auto wbuf = allocate_aligned_buffer<unsigned char>(alignment, alignment);
        std::fill(wbuf.get(), wbuf.get() + alignment, i);
        auto wb = wbuf.get();

        return f.dma_write(i * alignment, wb, alignment).then(
            [&f, i, wbuf = std::move(wbuf)](auto ret) mutable {
                BOOST_REQUIRE_EQUAL(ret, alignment);
                auto rbuf = allocate_aligned_buffer<unsigned char>(alignment, alignment);
                auto rb = rbuf.get();
                return f.dma_read(i * alignment, rb, alignment).then(
                    [f, rbuf = std::move(rbuf), wbuf = std::move(wbuf)](auto ret) {
                        BOOST_REQUIRE_EQUAL(ret, alignment);
                        BOOST_REQUIRE(std::equal(rbuf.get(), rbuf.get() + alignment, wbuf.get()));
                    });
            });
    }).wait();

    f.flush().wait();
    f.close().wait();
}
