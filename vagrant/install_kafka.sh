#!/bin/bash
cd /tmp

if [ ! -f '/tmp/kafka_2.8.0-0.8.1.1.tgz' ]; then
    wget http://mirrors.ircam.fr/pub/apache/kafka/0.8.1.1/kafka_2.8.0-0.8.1.1.tgz
fi

mkdir -p /opt/apache
cd /opt/apache

tar -xzf /tmp/kafka_2.8.0-0.8.1.1.tgz

if [ ! -h '/opt/apache/kafka' ]; then
	ln -s /opt/apache/kafka_2.8.0-0.8.1.1 kafka
fi