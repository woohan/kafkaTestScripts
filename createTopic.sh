#!/bin/bash

# $1 is the number of partitions, $2 is the number of replicas, $3 is the name

/home/apps/kafka/bin/kafka-topics.sh --create --config delete.retention.ms=300000 --bootstrap-server 172.23.0.2:9092,172.23.0.3:9092,172.23.0.4:9092 --partitions $1 --replication-factor $2 --topic $3
