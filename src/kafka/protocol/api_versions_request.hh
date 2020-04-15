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

#include <istream>
#include <ostream>

#include "api_versions_response.hh"

namespace seastar {

namespace kafka {

class api_versions_request {
public:
    using response_type = api_versions_response;
    static constexpr int16_t API_KEY = 18;
    static constexpr int16_t MIN_SUPPORTED_VERSION = 0;
    static constexpr int16_t MAX_SUPPORTED_VERSION = 2;

    void serialize(std::ostream& os, int16_t api_version) const;
    void deserialize(std::istream& is, int16_t api_version);
};

}

}
