#!/bin/sh
$KAFKA_HOME/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic impressions --partitions 1 --replication-factor 1