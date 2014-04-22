#!/bin/bash

#IP=$(ifconfig  | grep 'inet addr:'| grep 168 | grep 192|cut -d: -f2 | awk '{ print $1}')
IP=127.0.0.1

sed 's/#host.name=localhost/'host.name=$IP'/' /opt/apache/kafka/config/server.properties > /tmp/server.properties
sed 's/#advertised.host.name=<hostname routable by clients>/'advertised.host.name=$IP'/' /tmp/server.properties > /tmp/server.properties2
sed 's/num.partitions=2/num.partitions=1/' /tmp/server.properties2 > /opt/server.properties
echo "auto.leader.rebalance.enable=true" >> /opt/server.properties

## enable beta functionality
echo "delete.topic.enable=true" >> /opt/server.properties