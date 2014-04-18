package com.viadeo.axonframework.eventhandling.terminal.kafka;

import kafka.consumer.*;
import kafka.serializer.Decoder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.collection.mutable.ArraySeq;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
        when(Consumer.create(any(ConsumerConfig.class))).thenReturn(mock(ConsumerConnector.class));

        final ConsumerFactory consumerFactory = new ConsumerFactory(createDefaultConsumerConfig());
        final String category = "tourist";

        // When
        final ConsumerConnector consumer = consumerFactory.createConnector(category);

        // Then
        assertNotNull(consumer);

        consumer.shutdown();
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

    @SuppressWarnings("unchecked")
    @Test
    public void createStreams_withoutTopicFilterRegex_isOk() {
        // Given
        final ConsumerFactory consumerFactory = new ConsumerFactory(createDefaultConsumerConfig());
        final ConsumerConnector consumer = mock(ConsumerConnector.class);
        when(consumer.createMessageStreamsByFilter(any(TopicFilter.class), anyInt(), any(Decoder.class), any(Decoder.class))).thenReturn(new ArraySeq(0));

        // When
        consumerFactory.createStreams(1, consumer);

        // Then
        verify(consumer).createMessageStreamsByFilter(
                refEq(new Whitelist(".*")), anyInt(), any(Decoder.class), any(Decoder.class)
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void createStreams_withTopicFilterRegex_isOk() {
        // Given
        final String rawRegex = "foo.*";
        final ConsumerConfig consumerConfig = createDefaultConsumerConfig();
        consumerConfig.props().props().setProperty(ConsumerFactory.CONSUMER_TOPIC_FILTER_REGEX, rawRegex);

        final ConsumerFactory consumerFactory = new ConsumerFactory(consumerConfig);
        final ConsumerConnector consumer = mock(ConsumerConnector.class);
        when(consumer.createMessageStreamsByFilter(any(TopicFilter.class), anyInt(), any(Decoder.class), any(Decoder.class))).thenReturn(new ArraySeq(0));

        // When
        consumerFactory.createStreams(1, consumer);

        // Then
        verify(consumer).createMessageStreamsByFilter(
                refEq(new Whitelist(rawRegex)), anyInt(), any(Decoder.class), any(Decoder.class)
        );
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
