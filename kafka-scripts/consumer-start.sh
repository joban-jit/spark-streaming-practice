#!/bin/sh
$KAFKA_HOME/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic notifications --from-beginning