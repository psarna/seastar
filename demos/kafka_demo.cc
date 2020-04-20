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

#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/print.hh>
#include <seastar/core/thread.hh>
#include <seastar/kafka/producer/kafka_producer.hh>
#include <seastar/core/smp.hh>

using namespace seastar;

namespace bpo = boost::program_options;

seastar::future<sstring> async_stdin_read() {
    return seastar::smp::submit_to(1, []{
       sstring res;
       std::cin >> res;
       return res;
    });
}

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("host", bpo::value<std::string>()->default_value("172.13.0.1"), "Address of the Kafka broker")
        ("port", bpo::value<uint16_t>()->default_value(9092), "Port to connect through");

    return app.run(ac, av, [&app] {
        return seastar::async([&app] {
            auto&& config = app.configuration();
            std::string host = config["host"].as<std::string>();
            uint16_t port = config["port"].as<uint16_t>();
            (void) port;

            kafka::producer_properties properties;
            properties._client_id = "seastar-kafka-demo";
            properties._servers = {
                    {host, port}
            };

            kafka::kafka_producer producer(std::move(properties));
            producer.init().wait();
            fprint(std::cout, "Producer initialized and ready to send\n\n");

            sstring topic, key, value;
            while (true) {
                fprint(std::cout,
                       "\nType the topic and the message you want to send below. If you want to quit type 'q'\n");
                fprint(std::cout, "Enter topic: ");
                topic = async_stdin_read().get0();

                if (topic.empty() || topic == "q") {
                    producer.disconnect().wait();
                    fprint(std::cout, "Finished succesfully!\n");
                    break;
                }

                fprint(std::cout, "Enter key: ");
                key = async_stdin_read().get0();
                fprint(std::cout, "Enter value: ");
                value = async_stdin_read().get0();

                (void)producer.produce(topic, key, value).handle_exception([key, value](auto ep) {
                    fprint(std::cout, "Failure sending %s %s: %s.\n", key, value, ep);
                });
            }
        });
    });
}
