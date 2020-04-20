#!/usr/bin/env bash

# Producer test 3.
# Create 3 broker cluster and two topics with 3 partitions.
# Write 200 messages total to both topics.

source "./test_base_producer.sh"

init_kafka "3" "172.14.0.0/16"
init_topic "172.14.0.1:9092" "3" "3" "triple"
init_topic "172.14.0.1:9092" "3" "3" "triple2"
init_producer "172.14.0.1"
init_consumer "172.14.0.1:9092" "triple|triple2"

for i in {1..100}; do
    write_random "triple"
    write_random "triple2"
done

end_test "3" "172.14.0.0/16"
