#!/bin/sh
$KAFKA_HOME/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic invoice-items --from-beginning