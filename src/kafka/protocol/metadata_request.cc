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

#include "metadata_request.hh"

namespace seastar {

namespace kafka {

void metadata_request_topic::serialize(std::ostream& os, int16_t api_version) const {
    _name.serialize(os, api_version);
}

void metadata_request_topic::deserialize(std::istream& is, int16_t api_version) {
    _name.deserialize(is, api_version);
}

void metadata_request::serialize(std::ostream& os, int16_t api_version) const {
    _topics.serialize(os, api_version);
    if (api_version >= 4) {
        _allow_auto_topic_creation.serialize(os, api_version);
    }
    if (api_version >= 8) {
        _include_cluster_authorized_operations.serialize(os, api_version);
        _include_topic_authorized_operations.serialize(os, api_version);
    }
}

void metadata_request::deserialize(std::istream& is, int16_t api_version) {
    _topics.deserialize(is, api_version);
    if (api_version >= 4) {
        _allow_auto_topic_creation.deserialize(is, api_version);
    }
    if (api_version >= 8) {
        _include_cluster_authorized_operations.deserialize(is, api_version);
        _include_topic_authorized_operations.deserialize(is, api_version);
    }
}

}

}
