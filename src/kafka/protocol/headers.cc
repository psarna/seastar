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

#include "headers.hh"

namespace seastar {

namespace kafka {

void request_header::serialize(std::ostream& os, int16_t api_version) const {
    _api_key.serialize(os, api_version);
    _api_version.serialize(os, api_version);
    _correlation_id.serialize(os, api_version);
    _client_id.serialize(os, api_version);
}

void request_header::deserialize(std::istream& is, int16_t api_version) {
    _api_key.deserialize(is, api_version);
    _api_version.deserialize(is, api_version);
    _correlation_id.deserialize(is, api_version);
    _client_id.deserialize(is, api_version);
}

void response_header::serialize(std::ostream& os, int16_t api_version) const {
    _correlation_id.serialize(os, api_version);
}

void response_header::deserialize(std::istream& is, int16_t api_version) {
    _correlation_id.deserialize(is, api_version);
}

}

}
