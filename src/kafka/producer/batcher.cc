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
#include <seastar/core/sleep.hh>

namespace seastar {

namespace kafka {

void batcher::queue_message(sender_message message) {
    _messages_byte_size += message.size();
    _messages.emplace_back(std::move(message));
    if (_expiration_time == 0 || _messages_byte_size > _buffer_memory) {
        (void) flush();
    }
}

future<> batcher::flush() {
    return do_with(sender(_connection_manager, _metadata_manager, _request_timeout, _acks), [this](sender& sender) {
        bool is_batch_loaded = false;
        return _retry_helper.with_retry([this, &sender, is_batch_loaded]() mutable {
            // It is important to move messages from current batch
            // into sender and send requests in the same continuation,
            // in order to preserve correct order of messages.
            if (!is_batch_loaded) {
                for (auto& message : _messages) {
                    _messages_byte_size -= message.size();
                }
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

future<> batcher::flush_coroutine(std::chrono::milliseconds dur) {
    return seastar::do_until([this] { return !_keep_refreshing; }, [this, dur]{
        return seastar::sleep_abortable(dur, _stop_refresh).then([this] {
            return flush();
        }).handle_exception([] (std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (seastar::sleep_aborted& e) {
                return make_ready_future();
            } catch (...) {
                // no other exception should happen here,
                // if they do, they have to be handled individually
                std::rethrow_exception(ep);
            }
        });
    }).then([this]{
        _refresh_finished.signal();
    });
}

void batcher::start_flush() {
    if (_expiration_time == 0) {
        return;
    }
    _keep_refreshing = true;
    (void) flush_coroutine(std::chrono::milliseconds(_expiration_time));
}

future<> batcher::stop_flush() {
    if (_expiration_time == 0) {
        return make_ready_future();
    }
    _keep_refreshing = false;
    _stop_refresh.request_abort();
    return _refresh_finished.wait(1);
}

}

}