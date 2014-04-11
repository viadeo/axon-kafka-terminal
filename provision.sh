wget http://mirrors.ircam.fr/pub/apache/kafka/0.8.1/kafka_2.8.0-0.8.1.tgz
tar xzf kafka_2.8.0-0.8.1.tgz
./kafka_2.8.0-0.8.1/bin/kafka-server-start.sh -daemon ./kafka_2.8.0-0.8.1/config/server.properties
./kafka_2.8.0-0.8.1/bin/zookeeper-server-start.sh ./kafka_2.8.0-0.8.1/config/zookeeper.properties >/dev/null 2>&1 &
