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

#include "kafka_connection.hh"
#include "../protocol/metadata_response.hh"
#include "../protocol/metadata_request.hh"

#include <boost/functional/hash.hpp>

#include <unordered_map>

namespace seastar {

namespace kafka {

class connection_manager {
public:

    using connection_id = std::pair<std::string, uint16_t>;

private:

    std::unordered_map<connection_id, lw_shared_ptr<kafka_connection>, boost::hash<connection_id>> _connections;
    std::string _client_id;

    semaphore _connect_semaphore;
    semaphore _send_semaphore;

public:

    explicit connection_manager(std::string client_id)
        : _client_id(std::move(client_id)),
        _connect_semaphore(1),
        _send_semaphore(1) {}

    future<lw_shared_ptr<kafka_connection>> connect(const std::string& host, uint16_t port);
    std::optional<lw_shared_ptr<kafka_connection>> get_connection(const connection_id& connection);
    future<> disconnect(const connection_id& connection);

    template<typename RequestType>
    future<typename RequestType::response_type> send(RequestType request, const std::string& host, uint16_t port) {
        // In order to preserve ordering of sends, a semaphore with
        // count = 1 is used due to its FIFO guarantees.
        //
        // It is important that connect() and send() are done
        // with semaphore, as naive implementation
        //
        // connect(host, port).then([](auto conn) { conn.send(req1); });
        // connect(host, port).then([](auto conn) { conn.send(req2); });
        //
        // could introduce reordering of requests: after both
        // connects resolve as ready futures, the continuations (sends)
        // are not guaranteed to run with any specific order.
        //
        // In order to not limit concurrency, send_future is
        // returned as future<future<response>> and "unpacked"
        // outside the semaphore - scheduling inside semaphore
        // (only 1 at the time) and waiting for result outside it.
        return with_semaphore(_send_semaphore, 1, [this, request = std::move(request), host, port] {
            return connect(host, port).then([request = std::move(request)](auto conn) {
                auto send_future = conn->send(std::move(request)).finally([conn]{});
                return make_ready_future<decltype(send_future)>(std::move(send_future));
            });
        }).then([](auto send_future) {
            return send_future;
        }).handle_exception([] (auto ep) {
            // Handle connect exceptions.
            // TODO: Disconnect in case of broken connection.
            try {
                std::rethrow_exception(ep);
            } catch (seastar::timed_out_error& e) {
                typename RequestType::response_type response;
                response._error_code = error::kafka_error_code::REQUEST_TIMED_OUT;
                return response;
            } catch (...) {
                typename RequestType::response_type response;
                response._error_code = error::kafka_error_code::NETWORK_EXCEPTION;
                return response;
            }
        });
    }

    future<metadata_response> ask_for_metadata(const metadata_request& request);

};

}

}