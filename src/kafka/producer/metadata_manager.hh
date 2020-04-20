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

#include <chrono>

#include "../connection/connection_manager.hh"
#include <seastar/core/future.hh>
#include <seastar/core/abort_source.hh>

namespace seastar {

namespace kafka {

class metadata_manager {

private:
    connection_manager& _connection_manager;
    metadata_response _metadata;
    bool _keep_refreshing = false;
    semaphore _refresh_finished = 0;
    abort_source _stop_refresh;
    uint32_t _expiration_time;

public:
    explicit metadata_manager(connection_manager& manager, uint32_t expiration_time)
    : _connection_manager(manager), _expiration_time(expiration_time) {}

    seastar::future<> refresh_coroutine(std::chrono::milliseconds dur);
    seastar::future<> refresh_metadata();
    void start_refresh();
    future<> stop_refresh();
    // Capturing resulting metadata response object is forbidden,
    // it can be destroyed any time.
    metadata_response get_metadata();

};

}

}
