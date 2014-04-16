package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.google.common.base.Optional;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class ProducerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerFactory.class);
    private static Optional<String> brokerId = Optional.absent();

    private final Properties baseProperties;

    public ProducerFactory(final ProducerConfig producerConfig) {
        this.baseProperties = checkNotNull(producerConfig).props().props();
    }

    public <K, V> Producer<K, V> create() {
        final Properties properties = getPropertiesFor(getBrokerId());
        return new Producer<>(new ProducerConfig(properties));
    }

    protected Properties getPropertiesFor(final String brokerId) {
        final Properties properties = new Properties();

        properties.put("serializer.class", EventMessageSerializer.class.getName());
        properties.put("key.serializer.class", StringEncoder.class.getName());

        for (final Object key : baseProperties.keySet()) {
            properties.put(key, baseProperties.get(key));
        }

        properties.put("broker.id", brokerId);

        LOGGER.debug("Get properties for '{}' : {}", brokerId, properties);

        return properties;
    }

    protected String getBrokerId() {
        if ( ! brokerId.isPresent()) {
            try {
                brokerId = Optional.fromNullable(InetAddress.getLocalHost().getCanonicalHostName());
            } catch (UnknownHostException e) {
                brokerId = Optional.of(UUID.randomUUID().toString());
                LOGGER.warn("Unable to define 'broker.id' property. Generating default : {}", brokerId, e);
            }
        }
        return brokerId.get();
    }

}
