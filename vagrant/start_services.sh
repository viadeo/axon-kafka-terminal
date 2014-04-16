#!/bin/bash
/opt/apache/kafka/bin/zookeeper-server-start.sh /opt/apache/kafka/config/zookeeper.properties 1>> /tmp/zk.log 2>> /tmp/zk.log &
/opt/apache/kafka/bin/kafka-server-start.sh /opt/server.properties 1>> /tmp/broker.log 2>> /tmp/broker.log &
