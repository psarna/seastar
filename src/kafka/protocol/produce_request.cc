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

#include "produce_request.hh"

namespace seastar {

namespace kafka {

void produce_request_partition_produce_data::serialize(std::ostream& os, int16_t api_version) const {
    _partition_index.serialize(os, api_version);
    _records.serialize(os, api_version);
}

void produce_request_partition_produce_data::deserialize(std::istream& is, int16_t api_version) {
    _partition_index.deserialize(is, api_version);
    _records.deserialize(is, api_version);
}

void produce_request_topic_produce_data::serialize(std::ostream& os, int16_t api_version) const {
    _name.serialize(os, api_version);
    _partitions.serialize(os, api_version);
}

void produce_request_topic_produce_data::deserialize(std::istream& is, int16_t api_version) {
    _name.deserialize(is, api_version);
    _partitions.deserialize(is, api_version);
}

void produce_request::serialize(std::ostream& os, int16_t api_version) const {
    if (api_version >= 3) {
        _transactional_id.serialize(os, api_version);
    }
    _acks.serialize(os, api_version);
    _timeout_ms.serialize(os, api_version);
    _topics.serialize(os, api_version);
}

void produce_request::deserialize(std::istream& is, int16_t api_version) {
    if (api_version >= 3) {
        _transactional_id.deserialize(is, api_version);
    }
    _acks.deserialize(is, api_version);
    _timeout_ms.deserialize(is, api_version);
    _topics.deserialize(is, api_version);
}

}

}
