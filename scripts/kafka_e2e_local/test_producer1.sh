#!/usr/bin/env bash

# Producer test 1.
# Create 1 broker cluster and a single topic.
# Write a few messages to this topic.

source "./test_base_producer.sh"

init_kafka "1" "172.14.0.0/16"
init_topic "172.14.0.1:9092" "1" "1" "single"
init_producer "172.14.0.1"
init_consumer "172.14.0.1:9092" "single"

write_random "single"
write_random "single"
write_random "single"
end_test "1" "172.14.0.0/16"
