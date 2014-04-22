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
source /home/vagrant/src/vagrant/install_gradle.sh
su vagrant -c 'echo "export PATH=/opt/gradle-1.10/bin:$PATH" >> ~/.bashrc'

## Install Kafka with Zookeeper
source /home/vagrant/src/vagrant/install_kafka.sh

## Prepare
su vagrant -c 'source ~/.bashrc'

## Configure Kafka
source /home/vagrant/src/vagrant/configure_kafka.sh

## Launch services
su vagrant -c "source ~/src/vagrant/start_services.sh" 

sleep 10
