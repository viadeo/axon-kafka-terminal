package com.viadeo.axonframework.eventhandling.terminal.kafka;

import kafka.admin.DeleteTopicCommand;
import kafka.admin.TopicCommand;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.List;

public class TopicStatement {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicStatement.class);

    private final String zkConnect;

    public TopicStatement(final String zkConnect) {
        this.zkConnect = zkConnect;
    }

    public void create(final String... topics) {
        final ZkClient zkClient = createZkClient();
        final List<String> existingTopics = getTopics(zkClient);

        try {
            for (final String topic:topics) {
                if ( ! (existingTopics.contains(topic) || exists(zkClient, topic)) ) {

                    String[] args = {
                            "--zookeeper", zkConnect,
                            "--partitions", "1",
                            "--replication-factor", "1",
                            "--create",
                            "--topic", topic
                    };

                    final TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(args);
                    opts.checkArgs();

                    try {
                        TopicCommand.createTopic(zkClient, opts);
                    } catch (TopicExistsException e) {
                        LOGGER.warn("unable to create topic, the topic '{}' is already defined", topic, e);
                    }
                }
            }
        } finally {
            zkClient.close();
        }
    }

    public void remove(final String... topics) {
        final ZkClient zkClient = createZkClient();

        try {
            for (String topic:topics) {
                if (exists(zkClient, topic)) {

                    String[] args = {
                            "--zookeeper", zkConnect,
                            "--topic", topic
                    };

                    try {
                        DeleteTopicCommand.main(args);
                    } catch (Throwable e) {
                        LOGGER.warn("unable to delete topic: '{}", topic, e);
                    }
                }
            }
        } finally {
            zkClient.close();
        }
    }

    public ZkClient createZkClient(){
        return new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
    }

    public boolean exists(final ZkClient zkClient, final String topic) {
        return zkClient.exists(ZkUtils.getTopicPath(topic));
    }

    protected List<String> getTopics() {
        final ZkClient zkClient = createZkClient();
        try {
            return this.getTopics(zkClient);
        } finally {
            zkClient.close();
        }
    }

    protected List<String> getTopics(final ZkClient zkClient) {
        return JavaConversions.asList(ZkUtils.getAllTopics(zkClient));
    }

}
