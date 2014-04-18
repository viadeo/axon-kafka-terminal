package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.viadeo.axonframework.eventhandling.Shutdownable;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.EventBusTerminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EventBusTerminal implementation that uses Apache Kafka a messaging system to dispatch event messages. All outgoing
 * messages are sent by a configured Producer.
 * <p/>
 * This terminal does not dispatch Events internally, as it relies on each cluster to listen to it's own Kafka stream.
 */
public class KafkaTerminal implements EventBusTerminal, Shutdownable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTerminal.class);

    private final Map<String, KafkaClusterListener> clusterListenerByGroup;
    private final Producer<String,EventMessage> producer;
    private final ConsumerFactory consumerFactory;
    private final TopicStrategy topicStrategy;
    private final KafkaMetricHelper metricHelper;

    public KafkaTerminal(final ConsumerFactory consumerFactory, final ProducerFactory producerFactory) {
        this(
                consumerFactory,
                producerFactory,
                new DefaultTopicStrategy(),
                new KafkaMetricHelper(new MetricRegistry(), KafkaTerminal.class.getName().toLowerCase())
        );
    }

    public KafkaTerminal(
            final ConsumerFactory consumerFactory,
            final ProducerFactory producerFactory,
            final TopicStrategy topicStrategy,
            final KafkaMetricHelper metricHelper
    ) {
        this.metricHelper = metricHelper;
        this.consumerFactory = checkNotNull(consumerFactory);
        this.producer = checkNotNull(producerFactory).create();
        this.topicStrategy = checkNotNull(topicStrategy);
        this.clusterListenerByGroup = Maps.newHashMap();
    }

    @Override
    public void publish(final EventMessage... eventMessages) {
        checkNotNull(eventMessages);

        for (final EventMessage eventMessage : eventMessages) {
            final String topic = topicStrategy.getTopic(eventMessage);

            final KeyedMessage<String, EventMessage> message = new KeyedMessage<>(
                    topic,
                    eventMessage.getIdentifier(),
                    eventMessage
            );

            publish(message);
        }
    }

    protected void publish(final KeyedMessage<String, EventMessage> message) {
        final String event = message.message().getPayloadType().getSimpleName();
        final String topic = message.topic();

        try {
            producer.send(message);
            LOGGER.debug("Sent '{}' message with '{}' as identifier on the '{}' topic", event, message.key(), topic);

            metricHelper.markSentMessage(topic, event);
        } catch (Throwable t) {
            LOGGER.error("Unexpected error while sending '{}' message with '{}' as identifier on the '{}' topic", event, message.key(), topic, t);
            metricHelper.markErroredWhileSendingMessage(topic, event);
        }
    }

    @Override
    public void onClusterCreated(final Cluster cluster) {
        checkNotNull(cluster);

        final String group = cluster.getName();

        if ( ! clusterListenerByGroup.containsKey(group)) {
            clusterListenerByGroup.put(
                    group,
                    new KafkaClusterListener(consumerFactory, metricHelper, topicStrategy, cluster, 1)
            );
        }
    }

    protected List<KafkaClusterListener> getClusterListeners() {
        return Lists.newArrayList(clusterListenerByGroup.values());
    }

    @Override
    public void shutdown() {
        for (final KafkaClusterListener clusterListener : clusterListenerByGroup.values()) {
            clusterListener.shutdown();
        }
        producer.close();
    }

}
