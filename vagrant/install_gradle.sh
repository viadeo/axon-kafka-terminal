#!/bin/bash

cd /tmp
if [ ! -f '/tmp/gradle-1.10-bin.zip' ]; then
	wget http://services.gradle.org/distributions/gradle-1.10-bin.zip
fi

mkdir -p /opt
cd /opt

unzip -o /tmp/gradle-1.10-bin.zip

export PATH=/opt/gradle-1.10/bin:$PATH

