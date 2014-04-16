#!/bin/bash
sudo kill -9 `ps -ef | grep kafka | grep -v grep | awk '{print $2}'`