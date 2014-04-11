package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.codahale.metrics.MetricRegistry;

public class TerminalMetricHelper {

    private final MetricRegistry metricRegistry;
    private final String prefix;

    public TerminalMetricHelper(final MetricRegistry metricRegistry, final String prefix) {
        this.metricRegistry = metricRegistry;
        this.prefix = prefix;
    }

    public void markReceivedMessage(final String topic) {
        metricRegistry.meter(prefix + "." + topic + ".received_message").mark();
        metricRegistry.meter(prefix + ".received_message").mark();
    }

    public void markErroredWhileReceivingMessage(final String topic) {
        metricRegistry.meter(prefix + "." + topic + ".received_message_errors").mark();
        metricRegistry.meter(prefix + ".received_message_errors").mark();
    }

    public void markSentMessage(final String topic) {
        metricRegistry.meter(prefix + "." + topic + ".sent_message").mark();
        metricRegistry.meter(prefix + ".sent_message").mark();
    }

    public void markErroredWhileSendingMessage(final String topic) {
        metricRegistry.meter(prefix + "." + topic + ".sent_message_errors").mark();
        metricRegistry.meter(prefix + ".sent_message_errors").mark();
    }
}
