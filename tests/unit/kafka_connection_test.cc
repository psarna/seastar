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
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include "../../src/kafka/connection/tcp_connection.hh"

using namespace seastar;

// All of the tests below assume that there is a Kafka broker running
// on address BROKER_ADDRESS
constexpr char BROKER_ADDRESS[] = "172.13.0.1:9092";

constexpr char message_str[] = "\x00\x00\x00\x0E\x00\x12\x00\x02\x00\x00\x00\x00\x00\x04\x74\x65\x73\x74";
constexpr size_t message_len = 18;

SEASTAR_TEST_CASE(kafka_establish_connection_test) {
    return kafka::tcp_connection::connect(BROKER_ADDRESS).then([] (lw_shared_ptr<kafka::tcp_connection> conn) {
       return conn->close().finally([conn] {});
    }).then_wrapped([] (auto&& f) {
        try {
            f.get();
        } catch (std::exception& ex) {
            BOOST_FAIL(ex.what());
        }
    });
}

SEASTAR_TEST_CASE(kafka_connection_write_without_errors_test) {
    temporary_buffer<char> message {message_str, message_len};

    return kafka::tcp_connection::connect(BROKER_ADDRESS).then([message = std::move(message)] (lw_shared_ptr<kafka::tcp_connection> conn) {
        return conn->write(message.clone()).then([conn] {
            return conn->close().finally([conn] {});
        });
    }).then_wrapped([] (auto&& f) {
        try {
            f.get();
        } catch (std::exception& ex) {
            BOOST_FAIL(ex.what());
        }
    });
}

SEASTAR_TEST_CASE(kafka_connection_read_without_errors_test) {
    return make_ready_future();
}

SEASTAR_TEST_CASE(kafka_connection_successful_write_read_routine_test) {
    const std::string correct_response {"\x00\x00\x01\x1C\x00\x00\x00\x00\x00\x00\x00\x00\x00\x2d\x00\x00"
                                        "\x00\x00\x00\x07\x00\x01\x00\x00\x00\x0b\x00\x02\x00\x00\x00\x05"
                                        "\x00\x03\x00\x00\x00\x08\x00\x04\x00\x00\x00\x02\x00\x05\x00\x00"
                                        "\x00\x01\x00\x06\x00\x00\x00\x05\x00\x07\x00\x00\x00\x02\x00\x08"
                                        "\x00\x00\x00\x07\x00\x09\x00\x00\x00\x05\x00\x0a\x00\x00\x00\x02"
                                        "\x00\x0b\x00\x00\x00\x05\x00\x0c\x00\x00\x00\x03\x00\x0d\x00\x00"
                                        "\x00\x02\x00\x0e\x00\x00\x00\x03\x00\x0f\x00\x00\x00\x03\x00\x10"
                                        "\x00\x00\x00\x02\x00\x11\x00\x00\x00\x01\x00\x12\x00\x00\x00\x02"
                                        "\x00\x13\x00\x00\x00\x03\x00\x14\x00\x00\x00\x03\x00\x15\x00\x00"
                                        "\x00\x01\x00\x16\x00\x00\x00\x01\x00\x17\x00\x00\x00\x03\x00\x18"
                                        "\x00\x00\x00\x01\x00\x19\x00\x00\x00\x01\x00\x1a\x00\x00\x00\x01"
                                        "\x00\x1b\x00\x00\x00\x00\x00\x1c\x00\x00\x00\x02\x00\x1d\x00\x00"
                                        "\x00\x01\x00\x1e\x00\x00\x00\x01\x00\x1f\x00\x00\x00\x01\x00\x20"
                                        "\x00\x00\x00\x02\x00\x21\x00\x00\x00\x01\x00\x22\x00\x00\x00\x01"
                                        "\x00\x23\x00\x00\x00\x01\x00\x24\x00\x00\x00\x01\x00\x25\x00\x00"
                                        "\x00\x01\x00\x26\x00\x00\x00\x01\x00\x27\x00\x00\x00\x01\x00\x28"
                                        "\x00\x00\x00\x01\x00\x29\x00\x00\x00\x01\x00\x2a\x00\x00\x00\x01"
                                        "\x00\x2b\x00\x00\x00\x00\x00\x2c\x00\x00\x00\x00\x00\x00\x00\x00",
                                        18 * 16
    };

    temporary_buffer<char> message {message_str, message_len};

    return kafka::tcp_connection::connect(BROKER_ADDRESS).then(
        [message = std::move(message), correct_response] (lw_shared_ptr<kafka::tcp_connection> conn) {
            return conn->write(message.clone()).then([conn, correct_response] {
                return conn->read(18 * 16).then([conn, correct_response] (temporary_buffer<char> buff) {
                    std::string response {buff.get(), buff.size()};
                    BOOST_CHECK_EQUAL(response, correct_response);
                    return make_ready_future();
                }).then([conn] {
                    return conn->close().finally([conn] {});
                });
            });
        }).then_wrapped([] (auto&& f) {
        try {
            f.get();
        } catch (std::exception& ex) {
            BOOST_FAIL(ex.what());
        }
    });
}