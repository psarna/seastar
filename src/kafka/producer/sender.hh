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

#include <string>
#include <vector>
#include <map>
#include <seastar/core/future.hh>
#include "../protocol/metadata_response.hh"
#include "../protocol/produce_response.hh"
#include "../connection/connection_manager.hh"
#include "metadata_manager.hh"

namespace seastar {

namespace kafka {

struct send_exception : public std::runtime_error {
public:
    send_exception(const std::string& message) : runtime_error(message) {}
};

struct sender_message {
    std::string _key;
    std::string _value;

    std::string _topic;
    int32_t _partition_index;

    kafka_error_code_t _error_code;
    promise<> _promise;

    sender_message() :
        _partition_index(0),
        _error_code(error::kafka_error_code::UNKNOWN_SERVER_ERROR) {}
    sender_message(sender_message&& s) = default;
    sender_message& operator=(sender_message&& s) = default;
    sender_message(sender_message& s) = delete;
};

class sender {
public:
    using connection_id = std::pair<std::string, uint16_t>;
    using topic_partition = std::pair<std::string, int32_t>;

private:
    lw_shared_ptr<connection_manager> _connection_manager;
    lw_shared_ptr<metadata_manager> _metadata_manager;
    std::vector<sender_message> _messages;

    std::map<connection_id, std::map<std::string, std::map<int32_t, std::vector<sender_message*>>>> _messages_split_by_broker_topic_partition;
    std::map<topic_partition, std::vector<sender_message*>> _messages_split_by_topic_partition;
    std::vector<future<std::pair<connection_id, produce_response>>> _responses;

    std::optional<connection_id> broker_for_topic_partition(const std::string& topic, int32_t partition_index);
    connection_id broker_for_id(int32_t id);

    void set_error_code_for_broker(const connection_id& broker, const error::kafka_error_code& error_code);
    void set_error_code_for_topic_partition(const std::string& topic, int32_t partition_index,
            const error::kafka_error_code& error_code);
    void set_success_for_topic_partition(const std::string& topic, int32_t partition_index);

    void split_messages();
    void queue_requests();

    void set_error_codes_for_responses(std::vector<future<std::pair<connection_id, produce_response>>>& responses);
    void filter_messages();
    future<> process_messages_errors();
    
public:
    sender(lw_shared_ptr<connection_manager> connection_manager, lw_shared_ptr<metadata_manager> metadata_manager);

    void move_messages(std::vector<sender_message>& messages);
    size_t messages_size() const;
    bool messages_empty() const;

    void send_requests();
    future<> receive_responses();
    void close();
};

}

}
