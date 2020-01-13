#!/usr/bin/env bash

if [ -z ${KAFKA_DEMO_DIR+x} ]; then
    tput setaf 1
    echo "KAFKA_DEMO_DIR not set. Set environment variable KAFKA_DEMO_DIR to a directory of kafka_demo executable."
    tput sgr0
    exit 1
fi

if [ -z ${KAFKA_BIN_DIR+x} ]; then
    tput setaf 1
    echo "KAFKA_BIN_DIR not set. Set environment variable KAFKA_BIN_DIR to Kafka bin directory from Kafka sources,"
    echo "which contains console consumer and other tools."
    tput sgr0
    exit 1
fi

function init_kafka() {
    tput setaf 4
    echo "Starting Kafka."
    tput sgr0

    cd ../kafkadev_local || exit

    terraform init >/dev/null 2>&1
    terraform apply --var "kafka_count=$1" --var "network_cidr=$2" --auto-approve >/dev/null 2>&1

    cd "$OLDPWD"

    # Wait 10s for Kafka to start up.
    sleep 10s

    tput setaf 3
    printf "Started Kafka.\n\n"
    tput sgr0
}

function init_topic() {
    tput setaf 4
    echo "Creating topic: $4."
    tput sgr0

    "$KAFKA_BIN_DIR"/kafka-topics.sh --create --bootstrap-server "$1" --replication-factor "$2" \
        --partitions "$3" --topic "$4" >/dev/null 2>&1
}

function init_producer() {
    # Wait 15s for topics to init their leaders, etc.
    tput setaf 4
    echo "Waiting 15s for Kafka start up to fully finish."
    tput sgr0
    sleep 15s

    touch .kafka_producer_input

    # Set up netcat at port 7777 to pipe input into.
    (nc -k -l 7777 | "$KAFKA_DEMO_DIR"/kafka_demo --host "$1" >/dev/null 2>&1) &
    KAFKA_DEMO_PID=$!

    tput setaf 3
    printf "Started producer.\n\n"
    tput sgr0
}

function init_consumer() {
    "$KAFKA_BIN_DIR"/kafka-console-consumer.sh --bootstrap-server "$1" \
        --whitelist "$2" > .kafka_consumer_output 2>/dev/null &
    KAFKA_CONSUMER_PID=$!

    tput setaf 4
    echo "Starting consumer. Waiting 10s for it to start up."
    tput sgr0

    # Wait for consumer (and producer) to start up.
    # Mitigates against possible race conditions, Kafka leader not elected.
    sleep 10s

    tput setaf 3
    printf "Started consumer.\n\n"
    tput sgr0

    tput setaf 11
    echo "Starting test!"
    tput sgr0
}

function write_random() {
    RANDOM_LINE=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c 30 ; echo '')
    echo "$1" | nc localhost 7777
    echo "$RANDOM_LINE" | nc localhost 7777
    echo "$RANDOM_LINE" | nc localhost 7777
    echo "$RANDOM_LINE" >> .kafka_producer_input
}

function invoke_docker() {
    CONTAINERS=$(cd ../kafkadev_local; terraform output -json kafka_name | cut -c2- | rev | cut -c2- | rev | tr ',' '\n')
    NTH=$(echo $CONTAINERS | tr ' ' '\n' |  sed -n "$1p" | tr -d '"')

    tput setaf 4
    echo "Invoking docker: docker $2 $NTH."
    tput sgr0

    docker $2 $NTH >/dev/null 2>&1
}

function end_test() {
    tput setaf 11
    echo "Ending test!"
    tput sgr0

    tput setaf 4
    echo "Waiting 5s for processes to wind up."
    tput sgr0

    echo "q" | nc localhost 7777
    sleep 5s

    pkill -P $KAFKA_DEMO_PID >/dev/null 2>&1
    pkill -P $KAFKA_CONSUMER_PID >/dev/null 2>&1

    # Sort files, because partitions can change order of messages.
    sort -o .kafka_consumer_output .kafka_consumer_output
    sort -o .kafka_producer_input .kafka_producer_input

    if cmp -s .kafka_consumer_output .kafka_producer_input; then
        tput setaf 2
        echo "TEST PASSED! Consumer has received all messages from producer."
        tput sgr0
    else
        tput setaf 1
        echo "TEST FAILED! Consumer has NOT received all messages from producer."
        tput sgr0
    fi

    rm .kafka_producer_input
    rm .kafka_consumer_output

    cd ../kafkadev_local || exit

    terraform destroy --var "kafka_count=$1" --var "network_cidr=$2" --auto-approve >/dev/null 2>&1

    cd "$OLDPWD"
}
