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
 * Copyright (C) 2020 ScyllaDB
 */

#include "results_printers.hh"
#include "test_runner.hh"

#include <fmt/printf.h>
#include <fstream>
#include <seastar/json/formatter.hh>
#include <string>
#include <unordered_map>

namespace {

struct duration {
    double value;
};

[[maybe_unused]] static inline std::ostream& operator<<(std::ostream& os, duration d) {
    auto value = d.value;
    if (value < 1000) {
        os << fmt::format("{:.3f}ms", value);
    } else {
        os << fmt::format("{:.3f}s", value / 1000);
    }
    return os;
}

static constexpr auto format_string = "{:<40} {:>11} {:>11} {:>11} {:>11} {:>11}\n";

}

void stdout_printer::print_configuration() {
    fmt::print(format_string, "test", "runs", "median", "mad", "min", "max");
}

void stdout_printer::print_result(const result& r) {
    fmt::print(format_string, r.test_name, r.runs, ::duration { r.median },
            ::duration { r.mad }, ::duration { r.min }, ::duration { r.max });
}

json_printer::~json_printer() {
    std::ofstream out(_output_file);
    out << seastar::json::formatter::to_json(_root);
}

void json_printer::print_configuration() { }

void json_printer::print_result(const result& r) {
    auto& result = _root["results"][r.test_name];
    result["median"] = r.median;
    result["mad"] = r.mad;
    result["min"] = r.min;
    result["max"] = r.max;
}
