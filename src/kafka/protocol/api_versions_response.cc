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

#include "api_versions_response.hh"

namespace seastar {

namespace kafka {

void api_versions_response_key::serialize(kafka::output_stream &os, int16_t api_version) const {
    _api_key.serialize(os, api_version);
    _min_version.serialize(os, api_version);
    _max_version.serialize(os, api_version);
}

void api_versions_response_key::deserialize(kafka::input_stream &is, int16_t api_version) {
    _api_key.deserialize(is, api_version);
    _min_version.deserialize(is, api_version);
    _max_version.deserialize(is, api_version);
}

void api_versions_response::serialize(kafka::output_stream &os, int16_t api_version) const {
    _error_code.serialize(os, api_version);
    _api_keys.serialize(os, api_version);
    if (api_version >= 1) {
        _throttle_time_ms.serialize(os, api_version);
    }
}

void api_versions_response::deserialize(kafka::input_stream &is, int16_t api_version) {
    _error_code.deserialize(is, api_version);
    _api_keys.deserialize(is, api_version);
    if (api_version >= 1) {
        _throttle_time_ms.deserialize(is, api_version);
    }
}

}

}
