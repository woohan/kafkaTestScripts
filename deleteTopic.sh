#!/bin/bash

# $1 is the topic's name

/home/apps/kafka/bin/kafka-topics.sh --delete --bootstrap-server 172.23.0.2:9092,172.23.0.3:9092,172.23.0.4:9092 --topic $1
