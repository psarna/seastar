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

#include "../protocol/produce_request.hh"
#include <seastar/core/future-util.hh>
#include "sender.hh"

namespace seastar {

namespace kafka {

sender::sender(connection_manager& connection_manager,
        metadata_manager& metadata_manager,
        uint32_t connection_timeout,
        ack_policy acks)
            : _connection_manager(connection_manager),
            _metadata_manager(metadata_manager),
            _connection_timeout(connection_timeout),
            _acks(acks) {}

std::optional<sender::connection_id> sender::broker_for_topic_partition(const std::string& topic, int32_t partition_index) {
    // TODO: Improve complexity from O(N) to O(log N).
    auto metadata = _metadata_manager.get_metadata();
    for (const auto& current_topic : *metadata._topics) {
        if (*current_topic._name != topic) {
            continue;
        }
        if (current_topic._error_code != error::kafka_error_code::NONE) {
            continue;
        }
        for (const auto& current_partition : *current_topic._partitions) {
            if (*current_partition._partition_index == partition_index
                && current_partition._error_code == error::kafka_error_code::NONE) {
                return broker_for_id(*current_partition._leader_id);
            }
        }
    }
    return std::nullopt;
}

sender::connection_id sender::broker_for_id(int32_t id) {
    auto metadata = _metadata_manager.get_metadata();
    for (const auto& broker : *metadata._brokers) {
        if (*broker._node_id == id) {
            return {*broker._host, *broker._port};
        }
    }
    return {};
}

void sender::split_messages() {
    _messages_split_by_topic_partition.clear();
    _messages_split_by_broker_topic_partition.clear();
    for (auto& message : _messages) {
        auto broker = broker_for_topic_partition(message._topic, message._partition_index);
        if (broker) {
            _messages_split_by_broker_topic_partition[*broker][message._topic][message._partition_index].push_back(&message);
            _messages_split_by_topic_partition[{message._topic, message._partition_index}].push_back(&message);
        } else {
            // TODO: Differentiate between unknown topic, leader not available etc.
            message._error_code = error::kafka_error_code::UNKNOWN_TOPIC_OR_PARTITION;
        }
    }
}

void sender::queue_requests() {
    _responses.clear();
    for (auto& [broker, messages_by_topic_partition] : _messages_split_by_broker_topic_partition) {
        kafka::produce_request req;
        req._acks = static_cast<int16_t>(_acks);
        req._timeout_ms = 30000;

        kafka::kafka_array_t<kafka::produce_request_topic_produce_data> topics{
                std::vector<kafka::produce_request_topic_produce_data>()};
        req._topics = topics;

        for (auto& [topic, messages_by_partition] : messages_by_topic_partition) {
            kafka::produce_request_topic_produce_data topic_data;
            topic_data._name = topic;

            kafka::kafka_array_t<kafka::produce_request_partition_produce_data> partitions{
                    std::vector<kafka::produce_request_partition_produce_data>()};
            topic_data._partitions = partitions;

            for (auto& [partition, messages] : messages_by_partition) {
                kafka::produce_request_partition_produce_data partition_data;
                partition_data._partition_index = partition;

                kafka::kafka_records records;
                kafka::kafka_record_batch record_batch;

                record_batch._base_offset = 0;
                record_batch._partition_leader_epoch = -1;
                record_batch._magic = 2;
                record_batch._compression_type = kafka::kafka_record_compression_type::NO_COMPRESSION;
                record_batch._timestamp_type = kafka::kafka_record_timestamp_type::CREATE_TIME;

                auto first_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(messages[0]->_timestamp.time_since_epoch()).count();
                record_batch._first_timestamp = first_timestamp;
                record_batch._producer_id = -1;
                record_batch._producer_epoch = -1;
                record_batch._base_sequence = -1;
                record_batch._is_transactional = false;
                record_batch._is_control_batch = false;

                for (size_t i = 0; i < messages.size(); i++) {
                    auto current_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(messages[i]->_timestamp.time_since_epoch()).count();

                    kafka::kafka_record record;
                    record._timestamp_delta = current_timestamp - first_timestamp;
                    record._offset_delta = i;
                    record._key = messages[i]->_key;
                    record._value = messages[i]->_value;
                    record_batch._records.push_back(record);
                }

                records._record_batches.push_back(record_batch);
                partition_data._records = records;

                topic_data._partitions->push_back(partition_data);
            }
            req._topics->push_back(topic_data);
        }

        auto with_response = _acks != ack_policy::NONE;
        _responses.emplace_back(_connection_manager.send(req, broker.first, broker.second, _connection_timeout, with_response)
            .then([broker](auto response) {
                return std::make_pair(broker, response);
        }));
    }
}

void sender::set_error_code_for_broker(const sender::connection_id& broker, const error::kafka_error_code& error_code) {
    for (auto& [topic, messages_by_partition] : _messages_split_by_broker_topic_partition[broker]) {
        for (auto& [partition, messages] : messages_by_partition) {
            for (auto& message : messages) {
                (void)topic; (void)partition;
                message->_error_code = error_code;
            }
        }
    }
}

void sender::set_success_for_broker(const sender::connection_id& broker) {
    for (auto& [topic, messages_by_partition] : _messages_split_by_broker_topic_partition[broker]) {
        for (auto& [partition, messages] : messages_by_partition) {
            for (auto& message : messages) {
                (void)topic; (void)partition;
                message->_error_code = error::kafka_error_code::NONE;
                message->_promise.set_value();
            }
        }
    }
}

void sender::set_error_code_for_topic_partition(const std::string& topic, int32_t partition_index,
        const error::kafka_error_code& error_code) {
    for (auto& message : _messages_split_by_topic_partition[{topic, partition_index}]) {
        message->_error_code = error_code;
    }
}

void sender::set_success_for_topic_partition(const std::string& topic, int32_t partition_index) {
    for (auto& message : _messages_split_by_topic_partition[{topic, partition_index}]) {
        message->_error_code = error::kafka_error_code::NONE;
        message->_promise.set_value();
    }
}

void sender::move_messages(std::vector<sender_message>& messages) {
    _messages.insert(_messages.end(), std::make_move_iterator(messages.begin()),
              std::make_move_iterator(messages.end()));
    messages.clear();
}

size_t sender::messages_size() const {
    return _messages.size();
}

bool sender::messages_empty() const {
    return _messages.empty();
}

void sender::send_requests() {
    split_messages();
    queue_requests();
}

future<> sender::receive_responses() {
    return when_all(_responses.begin(), _responses.end()).then(
            [this](std::vector<future<std::pair<connection_id, produce_response>>> responses) {
        set_error_codes_for_responses(responses);
        filter_messages();
        return process_messages_errors();
    });
}

future<> sender::process_messages_errors() {
    auto should_refresh_metadata = false;
    for (auto& message : _messages) {
        if ((*message._error_code)._is_invalid_metadata) {
            should_refresh_metadata = true;
            break;
        }
    }
    if (should_refresh_metadata) {
        return _metadata_manager.refresh_metadata().discard_result();
    } else {
        return make_ready_future<>();
    }
}

void sender::filter_messages() {
    _messages.erase(std::remove_if(_messages.begin(), _messages.end(), [](auto& message) {
        if (message._error_code == error::kafka_error_code::NONE) {
            return true;
        }
        if (!(*message._error_code)._is_retriable) {
            message._promise.set_exception(send_exception((*message._error_code)._error_message));
            return true;
        }
        return false;
    }), _messages.end());
}

void sender::set_error_codes_for_responses(std::vector<future<std::pair<connection_id, produce_response>>>& responses) {
    for (auto& response : responses) {
        auto [broker, response_message] = response.get0();
        if (response_message._error_code != error::kafka_error_code::NONE) {
            set_error_code_for_broker(broker, *response_message._error_code);
            continue;
        }
        if (response_message._responses.is_null()) {
            // No detailed information (when ack_policy::NONE) so set success for the broker.
            set_success_for_broker(broker);
            continue;
        }
        for (auto& topic_response : *response_message._responses) {
            for (auto& partition_response : *topic_response._partitions) {
                if (partition_response._error_code == error::kafka_error_code::NONE) {
                    set_success_for_topic_partition(*topic_response._name, *partition_response._partition_index);
                } else {
                    set_error_code_for_topic_partition(*topic_response._name,
                                                       *partition_response._partition_index, *partition_response._error_code);
                }
            }
        }
    }
}

void sender::close() {
    for (auto& message : _messages) {
        message._promise.set_exception(send_exception((*message._error_code)._error_message));
    }
    _messages.clear();
}

}

}
