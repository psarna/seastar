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

#include "kafka_primitives.hh"
#include "kafka_records.hh"
#include "produce_response.hh"

namespace seastar {

namespace kafka {

class produce_request_partition_produce_data {
public:
    kafka_int32_t _partition_index;
    kafka_records _records;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class produce_request_topic_produce_data {
public:
    kafka_string_t _name;
    kafka_array_t<produce_request_partition_produce_data> _partitions;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class produce_request {
public:
    using response_type = produce_response;
    static constexpr int16_t API_KEY = 0;
    static constexpr int16_t MIN_SUPPORTED_VERSION = 2; // Kafka 0.10.0.0
    static constexpr int16_t MAX_SUPPORTED_VERSION = 8;

    kafka_nullable_string_t _transactional_id;
    kafka_int16_t _acks;
    kafka_int32_t _timeout_ms;
    kafka_array_t<produce_request_topic_produce_data> _topics;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

}

}
