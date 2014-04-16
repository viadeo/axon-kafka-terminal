package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.codahale.metrics.MetricRegistry;
import com.viadeo.axonframework.eventhandling.terminal.EventBusTerminalFactory;
import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;

public class KafkaTerminalFactory implements EventBusTerminalFactory {

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

    @Override
    public EventBusTerminalFactory with(final TopicStrategy topicStrategy) {
        this.topicStrategy = topicStrategy;
        return this;
    }

    public EventBusTerminalFactory with(final KafkaMetricHelper metricHelper) {
        this.metricHelper = metricHelper;
        return this;
    }

    @Override
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

