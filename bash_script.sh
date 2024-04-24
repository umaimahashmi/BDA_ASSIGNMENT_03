#!/bin/bash

# Function to start Zookeeper
start_zookeeper() {
    echo "Starting Zookeeper..."
    zookeeper-server-start.sh config/zookeeper.properties > /dev/null 2>&1 &
}

# Function to start Kafka broker
start_kafka() {
    echo "Starting Kafka broker..."
    kafka-server-start.sh config/server.properties > /dev/null 2>&1 &
}

# Function to start Kafka Connect (if needed)
start_kafka_connect() {
    echo "Starting Kafka Connect..."
    # Assuming Kafka Connect configuration file is located at config/connect-standalone.properties
    connect-standalone.sh config/connect-standalone.properties > /dev/null 2>&1 &
}


# Function to run producer
run_producer() {
    echo "Running producer script..."
    python3 producer.py
}

# Function to run consumer
run_consumer1() {
    echo "Running consumer script..."
    python3 apriori_consumer.py
}

# Main function
main() {
    start_zookeeper
    start_kafka
    start_kafka_connect  # Add this line if Kafka Connect is needed
    sleep 5  # Wait for Kafka to start (adjust as needed)
    run_producer &
    run_consumer1
}

# Execute main function
main

