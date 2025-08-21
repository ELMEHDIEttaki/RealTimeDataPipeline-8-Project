#!/bin/bash

# Wait kafka to be available
echo "Waiting for Kafka to become available..."
until kafka-topics.sh --bootstrap-server kafka:29092 --list &> /dev/null; do
    echo "Waiting for Kafka..."
    sleep 5
done

echo "Creating kafka topics..."

kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic engagement_events --replication-factor 1 --partitions 1

echo "Successfully created the following topics:"
kafka-topics.sh --bootstrap-server kafka:29092 --list