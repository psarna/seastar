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

#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_runner.hh>
#include "../../src/kafka/connection/tcp_connection.hh"

using namespace seastar;

// All of the tests below assume that there is a Kafka broker running
// on address BROKER_ADDRESS
constexpr char BROKER_ADDRESS[] = "172.13.0.1";
constexpr uint16_t PORT = 9092;
constexpr auto TIMEOUT = 1000;

constexpr char message_str[] = "\x00\x00\x00\x0E\x00\x12\x00\x02\x00\x00\x00\x00\x00\x04\x74\x65\x73\x74";
constexpr size_t message_len = sizeof(message_str);

SEASTAR_THREAD_TEST_CASE(kafka_establish_connection_test) {
    kafka::tcp_connection::connect(BROKER_ADDRESS, PORT, TIMEOUT).get();
}

SEASTAR_THREAD_TEST_CASE(kafka_connection_write_without_errors_test) {
    temporary_buffer<char> message {message_str, message_len};

    auto conn = kafka::tcp_connection::connect(BROKER_ADDRESS, PORT, TIMEOUT).get0();
    conn->write(message.clone()).get();
    conn->close().get();
}

SEASTAR_THREAD_TEST_CASE(kafka_connection_read_without_errors_test) {
    return;
}

SEASTAR_THREAD_TEST_CASE(kafka_connection_successful_write_read_routine_test) {
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

    auto conn = kafka::tcp_connection::connect(BROKER_ADDRESS, PORT, TIMEOUT).get0();
    conn->write(message.clone()).get();
    auto buff = conn->read(correct_response.length()).get0();
    std::string response {buff.get(), buff.size()};
    BOOST_CHECK_EQUAL(response, correct_response);
    conn->close().get();
}
