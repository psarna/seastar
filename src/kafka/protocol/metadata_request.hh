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
#include "metadata_response.hh"

namespace seastar {

namespace kafka {

class metadata_request_topic {
public:
    kafka_string_t _name;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class metadata_request {
public:
    using response_type = metadata_response;
    static constexpr int16_t API_KEY = 3;
    static constexpr int16_t MIN_SUPPORTED_VERSION = 1; // Kafka 0.10.0.0
    static constexpr int16_t MAX_SUPPORTED_VERSION = 8;

    kafka_array_t<metadata_request_topic> _topics;
    kafka_bool_t _allow_auto_topic_creation;
    kafka_bool_t _include_cluster_authorized_operations;
    kafka_bool_t _include_topic_authorized_operations;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

}

}
