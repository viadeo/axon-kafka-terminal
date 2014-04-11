package com.viadeo.axonframework.eventhandling.terminal.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

public class ProducerFactory {

    private final Properties baseProperties;

    public ProducerFactory(final ProducerConfig producerConfig) {
        this.baseProperties = checkNotNull(producerConfig).props().props();
    }

    public <K, V> Producer<K, V> create() {
        final Properties properties = getProperties();
        return new Producer<>(new ProducerConfig(properties));
    }

    protected Properties getProperties() {
        final Properties properties = new Properties();

        properties.put("serializer.class", EventMessageSerializer.class.getName());
        properties.put("key.serializer.class", StringEncoder.class.getName());

        for (final Object key : baseProperties.keySet()) {
            properties.put(key, baseProperties.get(key));
        }

        return properties;
    }


}
