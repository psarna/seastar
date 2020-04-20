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

#include <utility>
#include <vector>

#include "sender.hh"
#include "../utils/retry_helper.hh"

namespace seastar {

namespace kafka {

class batcher {
private:
    std::vector<sender_message> _messages;
    size_t _messages_byte_size;
    uint32_t _buffer_memory;
    metadata_manager& _metadata_manager;
    connection_manager& _connection_manager;
    retry_helper _retry_helper;
    ack_policy _acks;
    uint32_t _request_timeout;

    bool _keep_refreshing = false;
    semaphore _refresh_finished = 0;
    abort_source _stop_refresh;
    uint32_t _expiration_time;
public:
    batcher(metadata_manager& metadata_manager, connection_manager& connection_manager,
            uint32_t max_retries, ack_policy acks, uint32_t request_timeout, uint32_t expiration_time,
            uint32_t buffer_memory, noncopyable_function<future<>(uint32_t)> retry_strategy)
            : _messages_byte_size(0),
            _buffer_memory(buffer_memory),
            _metadata_manager(metadata_manager),
            _connection_manager(connection_manager),
            _retry_helper(max_retries, std::move(retry_strategy)),
            _acks(acks),
            _request_timeout(request_timeout),
            _expiration_time(expiration_time) {}

    void queue_message(sender_message message);
    future<> flush();
    future<> flush_coroutine(std::chrono::milliseconds dur);

    void start_flush();
    future<> stop_flush();
};

}

}
