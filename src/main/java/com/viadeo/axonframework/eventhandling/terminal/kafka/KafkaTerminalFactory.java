package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.codahale.metrics.MetricRegistry;
import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaTerminalFactory {

    private final ConsumerConfig consumerConfig;
    private final ProducerConfig producerConfig;

    private TopicStrategy topicStrategy;
    private KafkaMetricHelper metricHelper;

    public KafkaTerminalFactory(final Properties properties) {
        this(new ConsumerConfig(properties), new ProducerConfig(properties));
    }

    public KafkaTerminalFactory(final ConsumerConfig consumerConfig, final ProducerConfig producerConfig) {
        this.consumerConfig = consumerConfig;
        this.producerConfig = producerConfig;
    }

    public KafkaTerminalFactory with(final TopicStrategy topicStrategy) {
        this.topicStrategy = topicStrategy;
        return this;
    }

    public void setConsumerProperty(String key, Object value) {
        consumerConfig.props().props().setProperty(checkNotNull(key), checkNotNull(value).toString());
    }

    public void setProducerProperty(String key, Object value) {
        producerConfig.props().props().setProperty(checkNotNull(key), checkNotNull(value).toString());
    }

    public KafkaTerminalFactory with(final KafkaMetricHelper metricHelper) {
        this.metricHelper = metricHelper;
        return this;
    }

    public KafkaTerminal create() {
        if (topicStrategy == null) {
            this.topicStrategy = new DefaultTopicStrategy();
        }

        if (metricHelper == null) {
            this.metricHelper = new KafkaMetricHelper(new MetricRegistry(), KafkaTerminal.class.getName().toLowerCase());
        }

        return new KafkaTerminal(
                new ConsumerFactory(consumerConfig),
                new ProducerFactory(producerConfig),
                topicStrategy,
                metricHelper
        );
    }

    public static Properties from(final Map<String, String> map) {
        Properties properties = new Properties();
        properties.putAll(map);
        return properties;
    }
}

