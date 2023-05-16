#!/bin/sh
$KAFKA_HOME/bin/kafka-console-producer --topic sensor --bootstrap-server localhost:9092 --property "parse.key=true" --property "key.separator=:"