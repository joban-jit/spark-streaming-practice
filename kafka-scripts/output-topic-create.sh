#!/bin/sh
$KAFKA_HOME/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic notifications --partitions 1 --replication-factor 1