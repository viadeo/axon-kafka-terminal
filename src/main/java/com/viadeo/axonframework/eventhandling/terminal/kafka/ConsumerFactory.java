package com.viadeo.axonframework.eventhandling.terminal.kafka;

import kafka.consumer.*;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringEncoder;
import org.axonframework.domain.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ConsumerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerFactory.class);

    private final DefaultDecoder keyDecoder;
    private final EventMessageSerializer valueDecoder;
    private final ConsumerConfig baseConfig;

    public ConsumerFactory(final ConsumerConfig baseConfig) {
        this(new DefaultDecoder(null), new EventMessageSerializer(), baseConfig);
    }

    public ConsumerFactory(
            final DefaultDecoder keyDecoder,
            final EventMessageSerializer valueDecoder,
            final ConsumerConfig baseConfig
    ) {
        this.keyDecoder = checkNotNull(keyDecoder);
        this.valueDecoder = checkNotNull(valueDecoder);
        this.baseConfig = checkNotNull(baseConfig);
    }

    public ConsumerConnector createConnector(final String category) {
        checkNotNull(category);
        final Properties properties = getPropertiesFor(category);
        return Consumer.create(new ConsumerConfig(properties));
    }

    public List<KafkaStream<byte[], EventMessage>> createStreams(final int numStreams, final ConsumerConnector consumer){
        checkNotNull(consumer);
        checkState(numStreams > 0);
        return JavaConversions.asList(consumer.createMessageStreamsByFilter(
                new Whitelist(".*"),
                numStreams,
                keyDecoder,
                valueDecoder
        ));
    }

    protected Properties getPropertiesFor(final String category) {
        final Properties properties = new Properties();

        properties.put("serializer.class", EventMessageSerializer.class.getName());
        properties.put("key.serializer.class", StringEncoder.class.getName());

        for (final Object key : baseConfig.props().props().keySet()) {
            properties.put(key, baseConfig.props().props().get(key));
        }

        properties.put("group.id", checkNotNull(category));

        LOGGER.debug("Get properties for '{}' : {}", category, properties);

        return properties;
    }
}
