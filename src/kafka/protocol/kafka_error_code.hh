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

#include <string>
#include <cstdint>
#include <unordered_map>
#include <iostream>

namespace seastar {

namespace kafka {

namespace error {

class kafka_error_code {

public:

    int16_t _error_code;
    std::string _error_message;
    bool _is_retriable;
    bool _is_invalid_metadata;

    kafka_error_code(
        int16_t error_code,
        std::string error_message,
        bool is_retriable,
        bool is_invalid_metadata);

    static const kafka_error_code& get_error(int16_t value);

    static const kafka_error_code UNKNOWN_SERVER_ERROR;
    static const kafka_error_code NONE;
    static const kafka_error_code OFFSET_OUT_OF_RANGE;
    static const kafka_error_code CORRUPT_MESSAGE;
    static const kafka_error_code UNKNOWN_TOPIC_OR_PARTITION;
    static const kafka_error_code INVALID_FETCH_SIZE;
    static const kafka_error_code LEADER_NOT_AVAILABLE;
    static const kafka_error_code NOT_LEADER_FOR_PARTITION;
    static const kafka_error_code REQUEST_TIMED_OUT;
    static const kafka_error_code BROKER_NOT_AVAILABLE;
    static const kafka_error_code REPLICA_NOT_AVAILABLE;
    static const kafka_error_code MESSAGE_TOO_LARGE;
    static const kafka_error_code STALE_CONTROLLER_EPOCH;
    static const kafka_error_code OFFSET_METADATA_TOO_LARGE;
    static const kafka_error_code NETWORK_EXCEPTION;
    static const kafka_error_code COORDINATOR_LOAD_IN_PROGRESS;
    static const kafka_error_code COORDINATOR_NOT_AVAILABLE;
    static const kafka_error_code NOT_COORDINATOR;
    static const kafka_error_code INVALID_TOPIC_EXCEPTION;
    static const kafka_error_code RECORD_LIST__TOO_LARGE;
    static const kafka_error_code NOT_ENOUGH_REPLICAS;
    static const kafka_error_code NOT_ENOUGH_REPLICAS_AFTER_APPEND;
    static const kafka_error_code INVALID_REQUIRED_ACKS;
    static const kafka_error_code ILLEGAL_GENERATION;
    static const kafka_error_code INCONSISTENT_PROTOCOL;
    static const kafka_error_code INVALID_GROUP_ID;
    static const kafka_error_code UNKNOWN_MEMBER_ID;
    static const kafka_error_code INVALID_SESSION_TIMEOUT;
    static const kafka_error_code REBALANCE_IN_PROGRESS;
    static const kafka_error_code INVALID_COMMIT_OFFSET_SIZE;
    static const kafka_error_code TOPIC_AUTHORIZATION_FAILED;
    static const kafka_error_code GROUP_AUTHORIZATION_FAILED;
    static const kafka_error_code CLUSTER_AUTHORIZATION_FAILED;
    static const kafka_error_code INVALID_TIMESTAMP;
    static const kafka_error_code UNSUPPORTED_SASL_MECHANISM;
    static const kafka_error_code ILLEGAL_SASL_STATE;
    static const kafka_error_code UNSUPPORTED_VERSION;
    static const kafka_error_code TOPIC_ALREADY_EXISTS;
    static const kafka_error_code INVALID_PARTITIONS;
    static const kafka_error_code INVALID_REPLICATION_FACTOR;
    static const kafka_error_code INVALID_REPLICA_ASSIGNMENT;
    static const kafka_error_code INVALID_CONFIG;
    static const kafka_error_code NOT_CONTROLLER;
    static const kafka_error_code INVALID_REQUEST;
    static const kafka_error_code UNSUPPORTED_FOR_MESSAGE_FORMAT;
    static const kafka_error_code POLICY_VIOLATION;
    static const kafka_error_code OUT_OF_ORDER_SEQUENCE_NUMBER;
    static const kafka_error_code DUPLICATE_SEQUENCE_NUMBER;
    static const kafka_error_code INVALID_PRODUCER_EPOCH;
    static const kafka_error_code INVALID_TXN_STATE;
    static const kafka_error_code INVALID_PRODUCER_ID_MAPPING;
    static const kafka_error_code INVALID_TRANSACTION_TIMEOUT;
    static const kafka_error_code CONCURRENT_TRANSACTIONS;
    static const kafka_error_code TRANSACTION_COORDINATOR_FENCED;
    static const kafka_error_code TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
    static const kafka_error_code SECURITY_DISABLED;
    static const kafka_error_code OPERATION_NOT_ATTEMPTED;
    static const kafka_error_code KAFKA_STORAGE_ERROR;
    static const kafka_error_code LOG_DIR_NOT_FOUND;
    static const kafka_error_code SASL_AUTHENTICATION_FAILED;
    static const kafka_error_code UNKNOWN_PRODUCER_ID;
    static const kafka_error_code REASSIGNMENT_IN_PROGRESS;
    static const kafka_error_code DELEGATION_TOKEN_AUTH_DISABLED;
    static const kafka_error_code DELEGATION_TOKEN_NOT_FOUND;
    static const kafka_error_code DELEGATION_TOKEN_OWNER_MISMATCH;
    static const kafka_error_code DELEGATION_TOKEN_REQUEST_NOT_ALLOWED;
    static const kafka_error_code DELEGATION_TOKEN_AUTHORIZATION_FAILED;
    static const kafka_error_code DELEGATION_TOKEN_EXPIRED;
    static const kafka_error_code INVALID_PRINCIPAL_TYPE;
    static const kafka_error_code NON_EMPTY_GROUP;
    static const kafka_error_code GROUP_ID_NOT_FOUND;
    static const kafka_error_code FETCH_SESSION_ID_NOT_FOUND;
    static const kafka_error_code INVALID_FETCH_SESSION_EPOCH;
    static const kafka_error_code LISTENER_NOT_FOUND;
    static const kafka_error_code TOPIC_DELETION_DISABLED;
    static const kafka_error_code FENCED_LEADER_EPOCH;
    static const kafka_error_code UNKNOWN_LEADER_EPOCH;
    static const kafka_error_code UNSUPPORTED_COMPRESSION_TYPE;
    static const kafka_error_code STALE_BROKER_EPOCH;
    static const kafka_error_code OFFSET_NOT_AVAILABLE;
    static const kafka_error_code MEMBER_ID_REQUIRED;
    static const kafka_error_code PREFERRED_LEADER_NOT_AVAILABLE;
    static const kafka_error_code GROUP_MAX_SIZE_REACHED;
    static const kafka_error_code FENCED_INSTANCE_ID;
    static const kafka_error_code ELIGIBLE_LEADERS_NOT_AVAILABLE;
    static const kafka_error_code ELECTION_NOT_NEEDED;
    static const kafka_error_code NO_REASSIGNMENT_IN_PROGRESS;
    static const kafka_error_code GROUP_SUBSCRIBED_TO_TOPIC;
    static const kafka_error_code INVALID_RECORD;

private:
    static std::unordered_map<int16_t, const kafka_error_code&> errors;
};

}

}

}
