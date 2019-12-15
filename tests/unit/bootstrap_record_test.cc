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

#include "fs/bootstrap_record.hh"

#include <boost/crc.hpp>
#include <seastar/core/thread.hh>
#include <seastar/core/units.hh>
#include <seastar/fs/block_device.hh>
#include <seastar/fs/temporary_file.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace seastar;
using namespace seastar::fs;

namespace {

constexpr off_t device_size = 1 * MB;

class mock_block_device_impl : public block_device_impl {
public:
    using buf_type = basic_sstring<uint8_t, size_t, 32, false>;
    buf_type buf;
    ~mock_block_device_impl() override = default;

    future<size_t> write(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        if (buf.size() < pos + len)
            buf.resize(pos + len);
        std::memcpy(buf.data() + pos, buffer, len);
        return make_ready_future<size_t>(len);
    }

    future<size_t> read(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        std::memcpy(buffer, buf.data() + pos, len);
        return make_ready_future<size_t>(len);
    }

    future<> flush() override {
        return make_ready_future<>();
    }

    future<> close() override {
        return make_ready_future<>();
    }
};

future<block_device> prepare_device() {
    return async([&] {
        sstring device_path = [&]() -> sstring {
            temporary_file tmp_device_file("/tmp/bootstrap_record_test_file");
            return tmp_device_file.path();
        }();

        file f = open_file_dma(device_path, open_flags::rw | open_flags::create).get0();
        f.truncate(device_size).get();
        f.close().get();

        return open_block_device(device_path).get0();
    });
}

}

BOOST_TEST_DONT_PRINT_LOG_VALUE(bootstrap_record)
SEASTAR_TEST_CASE(test_valid_read_write) {
    return prepare_device().then([] (block_device dev) {
        return do_with(std::move(dev), [] (block_device& dev) {
            return async([&] {
                bootstrap_record write_record(1, 1024, 1024, 1, 3, {{6, {6, 9}}, {9, {9, 12}}, {12, {12, 15}}});
                // basic test
                write_record.write_to_disk(dev).get();
                auto read_record = bootstrap_record::read_from_disk(dev).get0();
                BOOST_REQUIRE_EQUAL(write_record, read_record);

                // max number of shards
                write_record.shards_nb = bootstrap_record::max_shards_nb;
                write_record.shards_info.resize(write_record.shards_nb, {1, {2, 3}});
                write_record.write_to_disk(dev).get();
                read_record = bootstrap_record::read_from_disk(dev).get0();
                BOOST_REQUIRE_EQUAL(write_record, read_record);

                // 0 shards
                write_record.shards_nb = 0;
                write_record.shards_info.clear();
                write_record.write_to_disk(dev).get();
                read_record = bootstrap_record::read_from_disk(dev).get0();
                BOOST_REQUIRE_EQUAL(write_record, read_record);
            }).finally([&] {
                return dev.close();
            });
        });
    });
}

namespace {

void repair_crc32(shared_ptr<mock_block_device_impl> dev_impl) {
    auto crc32 = [](const uint8_t* buff, size_t len) {
        boost::crc_32_type result;
        result.process_bytes(buff, len);
        return result.checksum();
    };
    mock_block_device_impl::buf_type& buff = dev_impl.get()->buf;
    size_t crc_pos = offsetof(bootstrap_record_disk, crc);
    uint32_t crc_new = crc32(buff.data(), crc_pos);
    std::memcpy(buff.data() + crc_pos, &crc_new, sizeof(crc_new));
}

void change_byte_at_offset(shared_ptr<mock_block_device_impl> dev_impl, size_t offset) {
    dev_impl.get()->buf[offset]++;
}

template<typename T>
void change_bytes_at_offset_to(shared_ptr<mock_block_device_impl> dev_impl, size_t offset, T value) {
    std::memcpy(dev_impl.get()->buf.data() + offset, &value, sizeof(value));
}

}

SEASTAR_THREAD_TEST_CASE(test_invalid_read) {
    auto dev_impl = make_shared<mock_block_device_impl>();
    block_device dev(dev_impl);
    bootstrap_record write_record(1, 1024, 1024, 1, 3, {{6, {6, 9}}, {9, {9, 12}}, {12, {12, 15}}});

    size_t crc_offset = offsetof(bootstrap_record_disk, crc);
    size_t magic_offset = offsetof(bootstrap_record_disk, magic);
    size_t shards_nb_offset = offsetof(bootstrap_record_disk, shards_nb);
    size_t sector_size_offset = offsetof(bootstrap_record_disk, sector_size);
    size_t cluster_size_offset = offsetof(bootstrap_record_disk, cluster_size);

    // invalid crc
    write_record.write_to_disk(dev).get();
    change_byte_at_offset(dev_impl, crc_offset);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) { return sstring(ex.what()) == "Read CRC is invalid"; });

    // invalid magic
    write_record.write_to_disk(dev).get();
    change_byte_at_offset(dev_impl, magic_offset);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) { return sstring(ex.what()) == "Read Magic Number is invalid"; });

    // invalid shards_nb
    write_record.write_to_disk(dev).get();
    change_bytes_at_offset_to(dev_impl, shards_nb_offset, bootstrap_record::max_shards_nb + 1);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return sstring(ex.what()) == "Read number of shards is bigger than max number of shards";
            });

    // sector_size not power of 2
    write_record.write_to_disk(dev).get();
    change_bytes_at_offset_to(dev_impl, sector_size_offset, 1234);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return sstring(ex.what()) == "Sector size should be a power of 2 and greater than 512";
            });

    // sector_size smaller than 512
    write_record.write_to_disk(dev).get();
    change_bytes_at_offset_to(dev_impl, sector_size_offset, 256);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return sstring(ex.what()) == "Sector size should be a power of 2 and greater than 512";
            });

    // cluster_size not divisible by sector_size
    write_record.write_to_disk(dev).get();
    change_bytes_at_offset_to(dev_impl, cluster_size_offset, 512);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return sstring(ex.what()) == "Cluster size should be a power of 2 and be divisible by sector size";
            });

    // cluster_size not power of 2
    write_record.write_to_disk(dev).get();
    change_bytes_at_offset_to(dev_impl, cluster_size_offset, 3072);
    repair_crc32(dev_impl);
    BOOST_CHECK_EXCEPTION(bootstrap_record::read_from_disk(dev).get(), invalid_bootstrap_record,
            [] (const invalid_bootstrap_record& ex) {
                return sstring(ex.what()) == "Cluster size should be a power of 2 and be divisible by sector size";
            });
}

SEASTAR_TEST_CASE(test_invalid_write) {
    return prepare_device().then([] (block_device dev) {
        return do_with(std::move(dev), [] (block_device& dev) {
            return async([&] {
                bootstrap_record write_record_default(1, 1024, 1024, 1, 0, {});
                bootstrap_record write_record = write_record_default;
                // shards_nb != shards_info.size()
                write_record.shards_nb = 2;
                write_record.shards_info.resize(3, {1, {2, 3}});
                BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
                        [] (const invalid_bootstrap_record& ex) {
                            return sstring(ex.what()) == "Shards number should match number of metadata pointers";
                        });

                // shards_nb > max_shards_nb
                write_record = write_record_default;
                write_record.shards_nb = bootstrap_record::max_shards_nb + 1;
                write_record.shards_info.resize(write_record.shards_nb, {1, {2, 3}});
                BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
                        [] (const invalid_bootstrap_record& ex) {
                            return sstring(ex.what()) == "Too many shards to fit into the bootstrap record";
                        });

                // sector_size not power of 2
                write_record = write_record_default;
                write_record.sector_size = 1234;
                BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
                        [] (const invalid_bootstrap_record& ex) {
                            return sstring(ex.what()) == "Sector size should be a power of 2 and greater than 512";
                        });

                // sector_size smaller than 512
                write_record = write_record_default;
                write_record.sector_size = 256;
                BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
                        [] (const invalid_bootstrap_record& ex) {
                            return sstring(ex.what()) == "Sector size should be a power of 2 and greater than 512";
                        });

                // cluster_size not divisible by sector_size
                write_record = write_record_default;
                write_record.cluster_size = 512;
                BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
                        [] (const invalid_bootstrap_record& ex) {
                            return sstring(ex.what()) == "Cluster size should be a power of 2 and be divisible by sector size";
                        });

                // cluster_size not power of 2
                write_record = write_record_default;
                write_record.cluster_size = 3072;
                BOOST_CHECK_EXCEPTION(write_record.write_to_disk(dev).get(), invalid_bootstrap_record,
                        [] (const invalid_bootstrap_record& ex) {
                            return sstring(ex.what()) == "Cluster size should be a power of 2 and be divisible by sector size";
                        });
            }).finally([&] {
                return dev.close();
            });
        });
    });
}
