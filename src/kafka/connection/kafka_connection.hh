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

#pragma once

#include "tcp_connection.hh"
#include "../protocol/headers.hh"
#include "../protocol/api_versions_request.hh"
#include "../protocol/api_versions_response.hh"

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>

namespace seastar {

namespace kafka {

class kafka_connection final {

    tcp_connection _connection;
    seastar::sstring _client_id;
    int32_t _correlation_id;
    api_versions_response _api_versions;
    semaphore _send_semaphore;
    semaphore _receive_semaphore;

    template<typename RequestType>
    temporary_buffer<char> serialize_request(RequestType request, int32_t correlation_id, int16_t api_version) {
        std::vector<char> header;
        boost::iostreams::back_insert_device<std::vector<char>> header_sink{header};
        boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> header_stream{header_sink};
        request_header req_header;
        req_header._api_key = RequestType::API_KEY;
        req_header._api_version = api_version;
        req_header._correlation_id = correlation_id;
        req_header._client_id = _client_id;
        req_header.serialize(header_stream, 0);
        header_stream.flush();

        std::vector<char> payload;
        boost::iostreams::back_insert_device<std::vector<char>> payload_sink{payload};
        boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> payload_stream{payload_sink};
        request.serialize(payload_stream, api_version);
        payload_stream.flush();

        std::vector<char> message;
        boost::iostreams::back_insert_device<std::vector<char>> message_sink{message};
        boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> message_stream{message_sink};

        kafka_int32_t message_size(header.size() + payload.size());
        message_size.serialize(message_stream, 0);
        message_stream.write(header.data(), header.size());
        message_stream.write(payload.data(), payload.size());
        message_stream.flush();

        return temporary_buffer<char>{message.data(), message.size()};
    }

    future<> send_request(temporary_buffer<char> message_buffer) {
        return _connection.write(std::move(message_buffer));
    }

    template<typename RequestType>
    future<typename RequestType::response_type> receive_response(int32_t correlation_id, int16_t api_version) {
        return _connection.read(4).then([] (temporary_buffer<char> response_size) {
            boost::iostreams::stream<boost::iostreams::array_source> response_size_stream
                    (response_size.get(), response_size.size());

            kafka_int32_t size;
            size.deserialize(response_size_stream, 0);
            return *size;
        }).then([this] (int32_t response_size) {
            return _connection.read(response_size);
        }).then([correlation_id, api_version] (temporary_buffer<char> response) {
            boost::iostreams::stream<boost::iostreams::array_source> response_stream
                    (response.get(), response.size());

            kafka::response_header response_header;
            response_header.deserialize(response_stream, 0);
            if (*response_header._correlation_id != correlation_id) {
                throw parsing_exception("Received invalid correlation id");
            }

            typename RequestType::response_type deserialized_response;
            deserialized_response.deserialize(response_stream, api_version);

            return deserialized_response;
        });
    }

    future<> init();

public:
    static future<lw_shared_ptr<kafka_connection>> connect(const seastar::sstring& host, uint16_t port,
            const seastar::sstring& client_id, uint32_t timeout_ms);

    kafka_connection(tcp_connection connection, seastar::sstring client_id) :
        _connection(std::move(connection)),
        _client_id(std::move(client_id)),
        _correlation_id(0),
        _send_semaphore(1),
        _receive_semaphore(1) {}

    kafka_connection(kafka_connection&& other) = default;
    kafka_connection(kafka_connection& other) = delete;

    future<> close();

    template<typename RequestType>
    future<typename RequestType::response_type> send(RequestType request) {
        return send(std::move(request), _api_versions.max_version<RequestType>());
    }

    template<typename RequestType>
    future<typename RequestType::response_type> send(RequestType request, int16_t api_version) {
        auto correlation_id = _correlation_id++;
        auto serialized_message = serialize_request(std::move(request), correlation_id, api_version);

        // In order to preserve ordering of sends, two semaphores with
        // count = 1 are used due to its FIFO guarantees.
        //
        // Send and receive are always queued jointly,
        // so that receive will get response from correct
        // request. Kafka guarantees that responses will
        // be sent in the same order that requests were sent.
        //
        // Usage of two semaphores makes it possible for
        // requests to be sent without waiting for
        // the previous response.
        auto request_future = with_semaphore(_send_semaphore, 1,
        [this, serialized_message = std::move(serialized_message)]() mutable {
            return send_request(std::move(serialized_message));
        });
        auto response_future = with_semaphore(_receive_semaphore, 1, [this, correlation_id, api_version] {
            return receive_response<RequestType>(correlation_id, api_version);
        }).handle_exception([] (std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (seastar::timed_out_error& e) {
                typename RequestType::response_type response;
                response._error_code = error::kafka_error_code::REQUEST_TIMED_OUT;
                return response;
            } catch (parsing_exception& e) {
                typename RequestType::response_type response;
                response._error_code = error::kafka_error_code::CORRUPT_MESSAGE;
                return response;
            } catch (...) {
                typename RequestType::response_type response;
                response._error_code = error::kafka_error_code::NETWORK_EXCEPTION;
                return response;
            }
        });
        return response_future;
    }

    template<typename RequestType>
    future<typename RequestType::response_type> send_without_response(RequestType request) {
        return send_without_response(std::move(request), _api_versions.max_version<RequestType>());
    }

    template<typename RequestType>
    future<typename RequestType::response_type> send_without_response(RequestType request, int16_t api_version) {
        auto correlation_id = _correlation_id++;
        auto serialized_message = serialize_request(std::move(request), correlation_id, api_version);

        auto request_future = with_semaphore(_send_semaphore, 1,
        [this, serialized_message = std::move(serialized_message)]() mutable {
            return send_request(std::move(serialized_message));
        }).then([] {
            typename RequestType::response_type response;
            response._error_code = error::kafka_error_code::NONE;
            return response;
        }).handle_exception([] (auto ep) {
            try {
                std::rethrow_exception(ep);
            } catch (seastar::timed_out_error& e) {
                typename RequestType::response_type response;
                response._error_code = error::kafka_error_code::REQUEST_TIMED_OUT;
                return response;
            } catch (parsing_exception& e) {
                typename RequestType::response_type response;
                response._error_code = error::kafka_error_code::CORRUPT_MESSAGE;
                return response;
            } catch (...) {
                typename RequestType::response_type response;
                response._error_code = error::kafka_error_code::NETWORK_EXCEPTION;
                return response;
            }
        });
        return request_future;
    }
};

}

}
