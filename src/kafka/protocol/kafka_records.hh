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

#include <vector>

namespace seastar {

namespace kafka {

class kafka_record_header {
public:
    seastar::sstring _header_key;
    seastar::sstring _value;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class kafka_record {
public:
    kafka_varint_t _timestamp_delta;
    kafka_varint_t _offset_delta;
    seastar::sstring _key;
    seastar::sstring _value;
    std::vector<kafka_record_header> _headers;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

enum class kafka_record_compression_type {
    NO_COMPRESSION = 0, GZIP = 1, SNAPPY = 2, LZ4 = 3, ZSTD = 4
};

enum class kafka_record_timestamp_type {
    CREATE_TIME = 0, LOG_APPEND_TIME = 1
};

class kafka_record_batch {
public:
    kafka_int64_t _base_offset;
    kafka_int32_t _partition_leader_epoch;
    kafka_int8_t _magic;

    kafka_record_compression_type _compression_type;
    kafka_record_timestamp_type _timestamp_type;
    bool _is_transactional;
    bool _is_control_batch;

    kafka_int64_t _first_timestamp;
    kafka_int64_t _producer_id;
    kafka_int16_t _producer_epoch;
    kafka_int32_t _base_sequence;

    std::vector<kafka_record> _records;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class kafka_records {
public:
    std::vector<kafka_record_batch> _record_batches;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

}

}
