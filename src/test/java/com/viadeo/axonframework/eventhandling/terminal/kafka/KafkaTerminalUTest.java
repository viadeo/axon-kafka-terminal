package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.google.common.collect.Sets;
import kafka.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.axonframework.domain.EventMessage;
import org.axonframework.domain.GenericEventMessage;
import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.EventListener;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class KafkaTerminalUTest {

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void publish_withNullAsEventMessages_throwException() {
        // Given
        final ConsumerFactory consumerFactory = mock(ConsumerFactory.class);

        final ProducerFactory producerFactory = mock(ProducerFactory.class);
        when(producerFactory.create()).thenReturn(mock(Producer.class));

        final KafkaTerminal kafkaTerminal = new KafkaTerminal(consumerFactory, producerFactory);

        // When
        kafkaTerminal.publish((EventMessage[])null);

        // Then throws an exception
    }

    @Test
    @SuppressWarnings("unchecked")
    public void publish_withSeveralEventMessages_isOk() {
        // Given
        final ConsumerFactory consumerFactory = mock(ConsumerFactory.class);

        final Producer<String, EventMessage> producer = mock(Producer.class);

        final ProducerFactory producerFactory = mock(ProducerFactory.class);
        when(producerFactory.<String, EventMessage>create()).thenReturn(producer);

        final KafkaTerminal kafkaTerminal = new KafkaTerminal(consumerFactory, producerFactory);

        final GenericEventMessage<String> firstMessage = new GenericEventMessage<>("1rst");
        final GenericEventMessage<String> secondMessage = new GenericEventMessage<>("2nd");

        // When
        kafkaTerminal.publish(firstMessage, secondMessage);

        // Then
        verify(producer, times(2)).send(any(KeyedMessage.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void publish_withOneEventMessage_isOk() {
        // Given
        final ConsumerFactory consumerFactory = mock(ConsumerFactory.class);

        final Producer<String, EventMessage> producer = mock(Producer.class);

        final ProducerFactory producerFactory = mock(ProducerFactory.class);
        when(producerFactory.<String, EventMessage>create()).thenReturn(producer);

        final KafkaTerminal kafkaTerminal = new KafkaTerminal(consumerFactory, producerFactory);

        final EventMessage message = mock(EventMessage.class);
        when(message.getIdentifier()).thenReturn("1");
        when(message.getPayloadType()).thenReturn(String.class);


        // When
        kafkaTerminal.publish(message);

        // Then
        verify(producer).send(refEq(new KeyedMessage<>(String.class.getName().toLowerCase(), "1", message)));
    }

    @Test(expected = NullPointerException.class)
    public void onClusterCreated_withNullAsCluster_throwException() {
        // Given
        final KafkaTerminal terminal = new KafkaTerminal(mock(ConsumerFactory.class), mock(ProducerFactory.class));

        // When
        terminal.onClusterCreated(null);

        // Then throws an exception
    }

    @Test
    public void onClusterCreated_withOneEventListener_createOneCluster() {
        // Given
        final String category = "fooBar";

        final ConsumerConnector consumerConnector = mock(ConsumerConnector.class);

        final ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
        when(consumerFactory.createConnector(category)).thenReturn(consumerConnector);

        final Cluster cluster = mock(Cluster.class);
        when(cluster.getName()).thenReturn(category);
        when(cluster.getMembers()).thenReturn(Sets.<EventListener>newHashSet(mock(EventListener.class)));

        final KafkaTerminal terminal = new KafkaTerminal(consumerFactory, mock(ProducerFactory.class));

        // When
        terminal.onClusterCreated(cluster);

        // Then
        assertEquals(1, terminal.getClusterListeners().size());
    }

    @Test
    public void onClusterCreated_withTwoEventListeners_createOneCluster() {
        // Given
        final String category = "fooBar";

        final ConsumerConnector consumerConnector = mock(ConsumerConnector.class);

        final ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
        when(consumerFactory.createConnector(category)).thenReturn(consumerConnector);

        final Cluster cluster = mock(Cluster.class);
        when(cluster.getName()).thenReturn(category);
        when(cluster.getMembers()).thenReturn(Sets.<EventListener>newHashSet(mock(EventListener.class), mock(EventListener.class)));

        final KafkaTerminal terminal = new KafkaTerminal(consumerFactory, mock(ProducerFactory.class));

        // When
        terminal.onClusterCreated(cluster);

        // Then
        assertEquals(1, terminal.getClusterListeners().size());
    }

}