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

#include "metadata_manager.hh"

namespace seastar {

namespace kafka {

    seastar::future<metadata_response> metadata_manager::refresh_metadata() {
        kafka::metadata_request req;

        req._allow_auto_topic_creation = true;
        req._include_cluster_authorized_operations = true;
        req._include_topic_authorized_operations = true;

        return _connection_manager->ask_for_metadata(req).then([this] (metadata_response metadata){
            return (_metadata = metadata);
        });
    }

    metadata_response& metadata_manager::get_metadata() {
        return _metadata;
    }
}

}
