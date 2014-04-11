package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.viadeo.axonframework.eventhandling.terminal.EventBusTerminalFactory;
import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;

public class KafkaTerminalFactory implements EventBusTerminalFactory {

    private final ConsumerConfig consumerConfig;
    private final ProducerConfig producerConfig;

    public KafkaTerminalFactory() {
        this(new Properties());
    }

    public KafkaTerminalFactory(final Properties properties) {
        this(new ConsumerConfig(properties), new ProducerConfig(properties));
    }

    public KafkaTerminalFactory(final ConsumerConfig consumerConfig, final ProducerConfig producerConfig) {
        this.consumerConfig = consumerConfig;
        this.producerConfig = producerConfig;
    }

    @Override
    public KafkaTerminal create() {
        return new KafkaTerminal(new ConsumerFactory(consumerConfig), new ProducerFactory(producerConfig));
    }

    public static Properties from(final Map<String, String> map) {
        Properties properties = new Properties();
        properties.putAll(map);
        return properties;
    }
}

