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

class kafka_connection {
private:
    lw_shared_ptr<tcp_connection> _connection;
    std::string _client_id;
    int32_t _correlation_id;
    api_versions_response _api_versions;

    template<typename RequestType>
    future<int32_t> send_request(const RequestType& request, int16_t api_version) {
        auto assigned_correlation_id = _correlation_id++;

        std::vector<char> header;
        boost::iostreams::back_insert_device<std::vector<char>> header_sink{header};
        boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> header_stream{header_sink};
        request_header req_header;
        req_header._api_key = RequestType::API_KEY;
        req_header._api_version = api_version;
        req_header._correlation_id = assigned_correlation_id;
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

        temporary_buffer<char> message_buffer{message.data(), message.size()};
        return _connection->write(message_buffer.clone())
                .then([assigned_correlation_id] { return assigned_correlation_id; });
    }

    template<typename RequestType>
    future<typename RequestType::response_type> receive_response(int32_t correlation_id, int16_t api_version) {
        return _connection->read(4).then([] (temporary_buffer<char> response_size) {
            boost::iostreams::stream<boost::iostreams::array_source> response_size_stream
                    (response_size.get(), response_size.size());

            kafka_int32_t size;
            size.deserialize(response_size_stream, 0);
            return *size;
        }).then([this] (int32_t response_size) {
            return _connection->read(response_size);
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
    static future<lw_shared_ptr<kafka_connection>> connect(const std::string& host, uint16_t port,
            const std::string& client_id);

    kafka_connection(lw_shared_ptr<tcp_connection> connection, std::string client_id) :
        _connection(std::move(connection)),
        _client_id(std::move(client_id)),
        _correlation_id(0) {}

    template<typename RequestType>
    future<typename RequestType::response_type> send(const RequestType& request) {
        return send(request, _api_versions.max_version<RequestType>());
    }

    template<typename RequestType>
    future<typename RequestType::response_type> send(const RequestType& request, int16_t api_version) {
        auto request_future = send_request(request, api_version);
        auto response_future = request_future.then([this, api_version] (int32_t correlation_id) {
            return receive_response<RequestType>(correlation_id, api_version);
        });
        return response_future;
    }
};

}

}
