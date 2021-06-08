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
 * Copyright (C) 2021 ScyllaDB
 */

#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/print.hh>
#include <seastar/core/memory.hh>
#include <seastar/util/std-compat.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/abort_source.hh>

#include <seastar/net/posix-stack.hh>

using namespace seastar;

int main(int ac, char** av) {
    return app_template().run(ac, av, [] {
        return seastar::async([&] {
            listen_options lo;
            lo.reuse_address = true;
            server_socket ss = seastar::listen(ipv4_addr("127.0.0.1", 1234), lo);

            abort_source as;
            auto client = async([&as] {
                connected_socket socket = connect(ipv4_addr("127.0.0.1", 1234)).get();
                socket.output().write("abc").get();
                socket.shutdown_output();
                try {
                    sleep_abortable(std::chrono::seconds(10), as).get();
                } catch (const sleep_aborted&) {
                    // expected
                    return;
                }
                assert(!"Skipping data from socket is likely stuck");
            });

            accept_result accepted = ss.accept().get();
            input_stream<char> input = accepted.connection.input();
            input.skip(16).get();
            as.request_abort();
            client.get();
            return 0;
        });
    });
}
