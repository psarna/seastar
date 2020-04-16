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
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

#include <algorithm>

namespace seastar {

namespace kafka {

    seastar::future<> metadata_manager::refresh_metadata() {
        kafka::metadata_request req;

        req._allow_auto_topic_creation = true;
        req._include_cluster_authorized_operations = true;
        req._include_topic_authorized_operations = true;

        return _connection_manager.ask_for_metadata(std::move(req)).then([this] (metadata_response metadata) {
            std::sort(metadata._brokers->begin(), metadata._brokers->end(), [] (auto& a, auto& b) {
                return *a._node_id < *b._node_id;
            });
            std::sort(metadata._topics->begin(), metadata._topics->end(), [] (auto& a, auto& b) {
                if (*a._name == *b._name) {
                    return a._error_code == error::kafka_error_code::NONE;
                } else {
                    return *a._name < *b._name;
                }
            });
            for (auto& topic : *metadata._topics) {
                std::sort(topic._partitions->begin(), topic._partitions->end(), [] (auto& a, auto& b) {
                    if (*a._partition_index == *b._partition_index) {
                        return a._error_code == error::kafka_error_code::NONE;
                    } else {
                        return *a._partition_index < *b._partition_index;
                    }
                });
            }
            _metadata = std::move(metadata);
        });
    }

    seastar::future<> metadata_manager::refresh_coroutine(std::chrono::milliseconds dur) {
        return seastar::do_until([this]{return ! _keep_refreshing;}, [this, dur]{
            return seastar::sleep_abortable(dur, _stop_refresh).then([this] {
                return refresh_metadata();
            }).handle_exception([this] (std::exception_ptr ep) {
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
            return;
        });
    }

    metadata_response metadata_manager::get_metadata() {
        return _metadata;
    }

    void metadata_manager::start_refresh() {
        _keep_refreshing = true;
        (void) refresh_coroutine(std::chrono::milliseconds(_expiration_time));
    }

    future<> metadata_manager::stop_refresh() {
        _keep_refreshing = false;
        _stop_refresh.request_abort();
        return _refresh_finished.wait(1);
    }
}

}
