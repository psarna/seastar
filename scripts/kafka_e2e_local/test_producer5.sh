#!/usr/bin/env bash

# Producer test 5.
# Create 3 broker cluster and two topics with 3 partitions.
# Write a few messages to both topics.
# Stop first broker.
# Write a few messages to both topics.
# Start first broker.

source "./test_base_producer.sh"

init_kafka "3" "172.14.0.0/16"
init_topic "172.14.0.1:9092" "3" "3" "triple"
init_topic "172.14.0.1:9092" "3" "3" "triple2"
init_producer "172.14.0.1"
init_consumer "172.14.0.1:9092" "triple|triple2"

for i in {1..10}; do
    write_random "triple"
    write_random "triple2"
done

sleep 5s
invoke_docker "1" "stop"
sleep 10s

for i in {1..10}; do
    write_random "triple"
    write_random "triple2"
done

sleep 5s
invoke_docker "1" "start"
sleep 10s

end_test "3" "172.14.0.0/16"
