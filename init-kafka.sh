#!/bin/bash

echo "Waiting for Kafka to be ready..."
sleep 5

echo "Creating Kafka topics..."
docker exec -it kafka-broker kafka-topics.sh \
    --create \
    --topic traffic-telemetry \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

echo "creating 'traffic-window-results' topic for Flink output..."
docker exec -it kafka-broker kafka-topics.sh \
    --create \
    --topic traffic-window-results \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

echo "Verification: Listing topics..."
docker exec -it kafka-broker kafka-topics.sh \
    --list \
    --bootstrap-server localhost:9092