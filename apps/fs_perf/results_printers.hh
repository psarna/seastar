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

#pragma once

#include "test_runner.hh"

#include <string>
#include <unordered_map>

struct stdout_printer final : result_printer {
    virtual void print_configuration() override;
    virtual void print_result(const result& r) override;
};

class json_printer final : public result_printer {
    std::string _output_file;
    std::unordered_map<std::string,
                       std::unordered_map<std::string,
                                          std::unordered_map<std::string, double>>> _root;
public:
    explicit json_printer(const std::string& file) : _output_file(file) { }
    ~json_printer();

    virtual void print_configuration() override;
    virtual void print_result(const result& r) override;
};
