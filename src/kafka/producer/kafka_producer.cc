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

#include <sstream>
#include <vector>
#include <iostream>

#include "../protocol/kafka_primitives.hh"
#include "../protocol/metadata_request.hh"
#include "../protocol/metadata_response.hh"
#include "../protocol/api_versions_request.hh"
#include "../protocol/api_versions_response.hh"
#include "../connection/tcp_connection.hh"
#include "../protocol/produce_request.hh"
#include "../protocol/produce_response.hh"

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>

#include <seastar/core/print.hh>
#include <seastar/kafka/producer/kafka_producer.hh>

namespace seastar {

namespace kafka {

kafka_producer::kafka_producer(std::string client_id) : _client_id(std::move(client_id)) {}

seastar::future<> kafka_producer::init(std::string server_address, uint16_t port) {
    auto connection_future = kafka_connection::connect(server_address, port, _client_id).then([this] (auto connection) {
        _connection = connection;
    });

    // TODO ApiVersions

    return connection_future.then([this] {
        return refresh_metadata();
    });
}

seastar::future<> kafka_producer::produce(std::string topic_name, std::string key, std::string value, int32_t partition_index) {
    kafka::produce_request req;
    req._acks = -1;
    req._timeout_ms = 30000;

    kafka::produce_request_topic_produce_data topic_data;
    topic_data._name = topic_name;

    kafka::produce_request_partition_produce_data partition_data;
    partition_data._partition_index = partition_index;

    kafka::kafka_records records;
    kafka::kafka_record_batch record_batch;

    record_batch._base_offset = 0;
    record_batch._partition_leader_epoch = -1;
    record_batch._magic = 2;
    record_batch._compression_type = kafka::kafka_record_compression_type::NO_COMPRESSION;
    record_batch._timestamp_type = kafka::kafka_record_timestamp_type::CREATE_TIME;
    record_batch._first_timestamp = 0x16e5b6eba2c; // TODO it should be a real time
    record_batch._producer_id = -1;
    record_batch._producer_epoch = -1;
    record_batch._base_sequence = -1;
    record_batch._is_transactional = false;
    record_batch._is_control_batch = false;

    kafka::kafka_record record;
    record._timestamp_delta = 0;
    record._offset_delta = 0;
    record._key = key;
    record._value = value;

    record_batch._records.push_back(record);
    records._record_batches.push_back(record_batch);

    partition_data._records = records;

    kafka::kafka_array_t<kafka::produce_request_partition_produce_data> partitions{
            std::vector<kafka::produce_request_partition_produce_data>()};
    partitions->push_back(partition_data);
    topic_data._partitions = partitions;

    kafka::kafka_array_t<kafka::produce_request_topic_produce_data> topics{
            std::vector<kafka::produce_request_topic_produce_data>()};
    topics->push_back(topic_data);

    req._topics = topics;

   return _connection->send(req).discard_result();
}

seastar::future<> kafka_producer::refresh_metadata() {
    kafka::metadata_request req;

    req._allow_auto_topic_creation = true;
    req._include_cluster_authorized_operations = true;
    req._include_topic_authorized_operations = true;

    return _connection->send(req).discard_result();
}

}

}
