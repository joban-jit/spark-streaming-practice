#!/bin/sh
$KAFKA_HOME/bin/kafka-topics --describe  --bootstrap-server localhost:9092 --topic nse-eod-topic