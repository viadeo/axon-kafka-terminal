package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.DefaultDecoder;
import org.axonframework.domain.EventMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith( PowerMockRunner.class )
@PrepareForTest( Consumer.class )
@PowerMockIgnore({"javax.management.*"})
public class ConsumerFactoryUTest {

    @Test(expected = NullPointerException.class)
    public void init_withNullAsConfiguration_throwException() throws Exception {
        // Given nothing

        // When
        new ConsumerFactory(null);

        // Then throws an exception
    }

    @Test(expected = NullPointerException.class)
    public void createConnector_withNullAsCategory_throwException() throws Exception {
        // Given
        final ConsumerFactory consumerFactory = new ConsumerFactory(createDefaultConsumerConfig());

        // When
        consumerFactory.createConnector(null);

        // Then throws an exception
    }

    @Test
    public void createConnector_withCategory_isOk() throws Exception {
        // Given
        mockStatic(Consumer.class);
        when(Consumer.createJavaConsumerConnector(any(ConsumerConfig.class))).thenReturn(mock(ConsumerConnector.class));

        final ConsumerFactory consumerFactory = new ConsumerFactory(createDefaultConsumerConfig());
        final String category = "tourist";

        // When
        final ConsumerConnector consumer = consumerFactory.createConnector(category);

        // Then
        assertNotNull(consumer);
    }

    @Test(expected = NullPointerException.class)
    public void getPropertiesFor_withNullAsCategory_throwException() {
        // Given
        final ConsumerFactory consumerFactory = new ConsumerFactory(createDefaultConsumerConfig());

        // When
        consumerFactory.getPropertiesFor(null);

        // Then throws an exception
    }

    @Test
    public void getPropertiesFor_withCategory_isOk() {
        // Given
        final ConsumerFactory consumerFactory = new ConsumerFactory(createDefaultConsumerConfig());
        final String category = "tourist";

        // When
        final Properties properties = consumerFactory.getPropertiesFor(category);

        // Then
        assertNotNull(properties);
        assertEquals(category, properties.get("group.id"));
        assertEquals("localhost:2181", properties.get("zookeeper.connect"));
        assertEquals("400", properties.get("zookeeper.session.timeout.ms"));
        assertEquals("200", properties.get("zookeeper.sync.time.ms"));
        assertEquals("1000", properties.get("auto.commit.interval.ms"));
    }

    @Test
    public void createStreams_withTopic_isOk() {
        // Given
        final String topic = "toto";
        final int nbThreads = 1;

        final Map<String, List<KafkaStream<byte[], EventMessage>>> streamsByTopic = ImmutableMap.<String, List<KafkaStream<byte[], EventMessage>>>builder()
                .put("toto", Lists.<KafkaStream<byte[], EventMessage>>newArrayList())
                .build();

        final Map<String, Integer> nbOfthreadByTopics = Maps.newHashMap();
        nbOfthreadByTopics.put(topic, nbThreads);

        final ConsumerConnector consumerConnector = mock(ConsumerConnector.class);
        when(
                consumerConnector.createMessageStreams(
                        refEq(nbOfthreadByTopics),
                        any(DefaultDecoder.class),
                        any(EventMessageSerializer.class)
                )
        ).thenReturn(streamsByTopic);

        final ConsumerFactory consumerFactory = new ConsumerFactory(createDefaultConsumerConfig());

        // When
        final Map<String,List<KafkaStream<byte[],EventMessage>>> streams = consumerFactory.createStreams(nbThreads, consumerConnector, topic);

        // Then throws an exception
        assertNotNull(streams);
        assertEquals(1, streams.size());
    }

    public static ConsumerConfig createDefaultConsumerConfig() {
        final Properties properties = new Properties();
        properties.put("group.id", "");
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("zookeeper.session.timeout.ms", "400");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(properties);
    }

}
