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

namespace seastar {

namespace kafka {

struct unsupported_version_exception : public std::runtime_error {
public:
    unsupported_version_exception(const std::string& message) : runtime_error(message) {}
};

class api_versions_response_key {
public:
    kafka_int16_t _api_key;
    kafka_int16_t _min_version;
    kafka_int16_t _max_version;

    bool operator<(const api_versions_response_key& other) const noexcept;
    bool operator<(int16_t api_key) const noexcept;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class api_versions_response {
public:
    kafka_error_code_t _error_code;
    kafka_array_t<api_versions_response_key> _api_keys;
    kafka_int32_t _throttle_time_ms;

    template<typename RequestType>
    int16_t max_version() const {
        auto broker_versions = (*this)[RequestType::API_KEY];
        if (*broker_versions._api_key == -1) {
            throw unsupported_version_exception("Broker does not support specific request");
        }
        if (*broker_versions._max_version < RequestType::MIN_SUPPORTED_VERSION) {
            throw unsupported_version_exception("Broker is too old");
        }
        if (*broker_versions._min_version > RequestType::MAX_SUPPORTED_VERSION) {
            throw unsupported_version_exception("Broker is too new");
        }
        return std::min(*broker_versions._max_version, RequestType::MAX_SUPPORTED_VERSION);
    }
    bool contains(int16_t api_key) const;
    api_versions_response_key operator[](int16_t api_key) const;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

}

}
