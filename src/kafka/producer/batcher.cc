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

#include "batcher.hh"

namespace seastar {

namespace kafka {

void batcher::queue_message(sender_message message) {
    _messages.emplace_back(std::move(message));
}

future<> batcher::flush() {
    return do_with(sender(_connection_manager, _metadata_manager), [this](sender& sender) {
        bool is_batch_loaded = false;
        return _retry_helper.with_retry([this, &sender, is_batch_loaded]() mutable {
            // It is important to move messages from current batch
            // into sender and send requests in the same continuation,
            // in order to preserve correct order of messages.
            if (!is_batch_loaded) {
                sender.move_messages(_messages);
                is_batch_loaded = true;
            }
            sender.send_requests();

            return sender.receive_responses().then([&sender] {
                return sender.messages_empty() ? do_retry::no : do_retry::yes;
            });
        }).finally([&sender] {
            return sender.close();
        });
    });
}

}

}