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

#include "partitioner.hh"
#include <boost/functional/hash.hpp>

namespace seastar {

namespace kafka {

metadata_response_partition basic_partitioner::get_partition(const seastar::sstring& key, const kafka_array_t<metadata_response_partition>& partitions) {
    size_t index = std::rand() % partitions->size();
    return partitions[index];
}

metadata_response_partition rr_partitioner::get_partition(const seastar::sstring& key, const kafka_array_t<metadata_response_partition>& partitions) {
    if(!key.empty()) {
        auto hash_value = std::hash<sstring>()(key);
        return partitions[hash_value % partitions->size()];
    }
    else {
        return partitions[(counter++) % partitions->size()];
    }
}

}

}