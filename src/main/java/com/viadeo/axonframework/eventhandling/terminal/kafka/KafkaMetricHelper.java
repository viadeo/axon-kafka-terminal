package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.codahale.metrics.MetricRegistry;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaMetricHelper {

    private final MetricRegistry metricRegistry;
    private final String prefix;

    public KafkaMetricHelper(final MetricRegistry metricRegistry, final String prefix) {
        this.metricRegistry = checkNotNull(metricRegistry);
        this.prefix = checkNotNull(prefix);
    }

    public void markReceivedMessage(final String event) {
        metricRegistry.meter(prefix + ".consumer.event." + event + ".receive").mark();
        metricRegistry.meter(prefix + ".consumer.receive").mark();
    }

    public void markErroredWhileReceivingMessage(final String event) {
        metricRegistry.meter(prefix + ".consumer.event." + event + ".error").mark();
        metricRegistry.meter(prefix + ".consumer.error").mark();
    }

    public void markSentMessage(final String topic, final String event) {
        metricRegistry.meter(prefix + ".producer.topic." + topic + ".send").mark();
        metricRegistry.meter(prefix + ".producer.event." + event + ".send").mark();
        metricRegistry.meter(prefix + ".producer.send").mark();
    }

    public void markErroredWhileSendingMessage(final String topic, final String event) {
        metricRegistry.meter(prefix + ".producer.topic." + topic + ".error").mark();
        metricRegistry.meter(prefix + ".producer.event." + event + ".error").mark();
        metricRegistry.meter(prefix + ".producer.error").mark();
    }
}
