#!/bin/bash

## Install tools
apt-get -y update
apt-get install -y python-software-properties wget unzip

## Install Java7
add-apt-repository -y ppa:webupd8team/java
apt-get -y update
/bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get -y install oracle-java7-installer oracle-java7-set-default

su vagrant -c 'touch ~/.bashrc'
su vagrant -c 'echo "export JAVA_HOME=$( dirname $( dirname $( readlink -e $(which javac) ) ) )" >> ~/.bashrc'

## Install Gradle
cd /tmp
if [ ! -f '/tmp/gradle-1.10-bin.zip' ]; then
	wget http://services.gradle.org/distributions/gradle-1.10-bin.zip
fi
mkdir -p /opt
cd /opt
unzip -o /tmp/gradle-1.10-bin.zip
export PATH=/opt/gradle-1.10/bin:$PATH

su vagrant -c 'echo "export PATH=/opt/gradle-1.10/bin:$PATH" >> ~/.bashrc'

## Install Kafka with Zookeeper
cd /tmp
if [ ! -f '/tmp/kafka_2.8.0-0.8.1.tgz' ]; then
    wget http://mirrors.ircam.fr/pub/apache/kafka/0.8.1/kafka_2.8.0-0.8.1.tgz
fi
mkdir -p /opt/apache
cd /opt/apache
tar -xzf /tmp/kafka_2.8.0-0.8.1.tgz
if [ ! -h '/opt/apache/kafka' ]; then
	ln -s /opt/apache/kafka_2.8.0-0.8.1 kafka
fi

## Finnally
su vagrant -c 'source ~/.bashrc'

## Launch services
#IP=$(ifconfig  | grep 'inet addr:'| grep 168 | grep 192|cut -d: -f2 | awk '{ print $1}')
IP=127.0.0.1
sed 's/#host.name=localhost/'host.name=$IP'/' /opt/apache/kafka/config/server.properties > /tmp/server.properties
sed 's/#advertised.host.name=<hostname routable by clients>/'advertised.host.name=$IP'/' /tmp/server.properties > /tmp/server.properties2 
sed 's/num.partitions=2/num.partitions=1/' /tmp/server.properties2 > /opt/server.properties
echo "auto.leader.rebalance.enable=true" >> /opt/server.properties
## enable beta functionality
echo "delete.topic.enable=true" >> /opt/server.properties

su vagrant -c "source ~/src/vagrant/start_services.sh" 

sleep 10
