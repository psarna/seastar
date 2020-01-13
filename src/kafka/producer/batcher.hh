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

#include <utility>
#include <vector>

#include "sender.hh"
#include "../utils/retry_helper.hh"

namespace seastar {

namespace kafka {

class batcher {
private:
    std::vector<sender_message> _messages;
    lw_shared_ptr<metadata_manager> _metadata_manager;
    lw_shared_ptr<connection_manager> _connection_manager;
    retry_helper _retry_helper;
public:
    batcher(lw_shared_ptr<metadata_manager> metadata_manager,
            lw_shared_ptr<connection_manager> connection_manager)
            : _metadata_manager(std::move(metadata_manager)),
            _connection_manager(std::move(connection_manager)),
            _retry_helper(60, 20, 1000) {}

    void queue_message(sender_message message);
    future<> flush();
};

}

}
