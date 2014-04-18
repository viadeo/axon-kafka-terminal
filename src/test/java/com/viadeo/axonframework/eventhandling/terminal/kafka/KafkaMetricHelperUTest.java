package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class KafkaMetricHelperUTest {

    private Meter meter;
    private MetricRegistry metricRegistry;

    @Before
    public void setUp() {
        meter = mock(Meter.class);

        metricRegistry = mock(MetricRegistry.class);
        when(metricRegistry.meter(anyString())).thenReturn(meter);
    }

    @Test
    public void markReceivedMessage_markTwoMeters() {
        // Given
        final String prefix = "foo";
        final KafkaMetricHelper kafkaMetricHelper = new KafkaMetricHelper(metricRegistry, prefix);

        // When
        kafkaMetricHelper.markReceivedMessage("bar");

        // Then
        verify(metricRegistry).meter("foo.consumer.event.bar.receive");
        verify(metricRegistry).meter("foo.consumer.receive");
        verify(meter, times(2)).mark();
    }

    @Test
    public void markErroredWhileReceivingMessage_markTwoMeters() {
        // Given
        final String prefix = "foo";
        final KafkaMetricHelper kafkaMetricHelper = new KafkaMetricHelper(metricRegistry, prefix);

        // When
        kafkaMetricHelper.markErroredWhileReceivingMessage("bar");

        // Then
        verify(metricRegistry).meter("foo.consumer.event.bar.error");
        verify(metricRegistry).meter("foo.consumer.error");
        verify(meter, times(2)).mark();
    }

    @Test
    public void markSentMessage_markTwoMeters() {
        // Given
        final String prefix = "foo";
        final KafkaMetricHelper kafkaMetricHelper = new KafkaMetricHelper(metricRegistry, prefix);

        // When
        kafkaMetricHelper.markSentMessage("bar", "hoy");

        // Then
        verify(metricRegistry).meter("foo.producer.topic.bar.send");
        verify(metricRegistry).meter("foo.producer.event.hoy.send");
        verify(metricRegistry).meter("foo.producer.send");
        verify(meter, times(3)).mark();
    }

    @Test
    public void markErroredWhileSendingMessage_markTwoMeters() {
        // Given
        final String prefix = "foo";
        final KafkaMetricHelper kafkaMetricHelper = new KafkaMetricHelper(metricRegistry, prefix);

        // When
        kafkaMetricHelper.markErroredWhileSendingMessage("bar", "hoy");

        // Then
        verify(metricRegistry).meter("foo.producer.topic.bar.error");
        verify(metricRegistry).meter("foo.producer.event.hoy.error");
        verify(metricRegistry).meter("foo.producer.error");
        verify(meter, times(3)).mark();
    }
}
