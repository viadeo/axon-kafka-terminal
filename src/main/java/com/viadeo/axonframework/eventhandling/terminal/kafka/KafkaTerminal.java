package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.viadeo.axonframework.eventhandling.Shutdownable;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.EventBusTerminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EventBusTerminal implementation that uses Apache Kafka a messaging system to dispatch event messages. All outgoing
 * messages are sent by a configured Producer.
 * <p/>
 * This terminal does not dispatch Events internally, as it relies on each cluster to listen to it's own Kafka stream.
 */
public class KafkaTerminal implements EventBusTerminal, Shutdownable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTerminal.class);

    private final Map<String, ConsumerConnector> consumerByCategory;
    private final Producer<String,EventMessage> producer;
    private final ConsumerFactory consumerFactory;
    private final TopicNormalizer topicNormalizer;
    private final TerminalMetricHelper metricHelper;

    public KafkaTerminal(final ConsumerFactory consumerFactory, final ProducerFactory producerFactory) {
        this(
                consumerFactory,
                producerFactory,
                new TopicNormalizer(),
                new TerminalMetricHelper(new MetricRegistry(), KafkaTerminal.class.getName().toLowerCase())
        );
    }

    public KafkaTerminal(
            final ConsumerFactory consumerFactory,
            final ProducerFactory producerFactory,
            final TopicNormalizer topicNormalizer,
            final TerminalMetricHelper metricHelper
    ) {
        this.metricHelper = metricHelper;
        this.consumerFactory = checkNotNull(consumerFactory);
        this.producer = checkNotNull(producerFactory).create();
        this.topicNormalizer = checkNotNull(topicNormalizer);
        this.consumerByCategory = Maps.newHashMap();
    }

    @Override
    public void publish(final EventMessage... eventMessages) {
        checkNotNull(eventMessages);

        for (final EventMessage eventMessage : eventMessages) {
            final String topic = topicNormalizer.normalize(eventMessage.getPayloadType().getName());

            final KeyedMessage<String, EventMessage> message = new KeyedMessage<>(
                    topic,
                    eventMessage.getIdentifier(),
                    eventMessage
            );

            try {
                producer.send(message);
                LOGGER.debug("Sent message with '{}' as identifier on the '{}' topic", eventMessage.getIdentifier(), topic);

                metricHelper.markSentMessage(topic);
            } catch (Throwable t) {
                LOGGER.error("Unexpected error while sending a message with '{}' as identifier on the '{}' topic", eventMessage.getIdentifier(), topic, t);
                metricHelper.markErroredWhileSendingMessage(topic);
            }
        }
    }

    @Override
    public void onClusterCreated(final Cluster cluster) {
        checkNotNull(cluster);

        final String category = cluster.getName();

        ConsumerConnector consumer = consumerByCategory.get(category);

        if(null == consumer) {
            consumer = consumerFactory.createConnector(category);
            this.consumerByCategory.put(category, consumer);
        }

        final int numStreams = 1;
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("event-consumer-" + category + "-%d").build();
        final List<KafkaStream<byte[], EventMessage>> streams = consumerFactory.createStreams(numStreams, consumer);
        final ExecutorService executorService = Executors.newFixedThreadPool(numStreams, threadFactory);

        for (final KafkaStream<byte[], EventMessage> stream : streams) {
            executorService.submit(new KafkaStreamListener(cluster, stream, metricHelper, topicNormalizer));
        }
    }

    protected List<ConsumerConnector> getConsumers() {
        return Lists.newArrayList(consumerByCategory.values());
    }

    @Override
    public void shutdown() {
        for (final ConsumerConnector consumerConnector : consumerByCategory.values()) {
            consumerConnector.shutdown();
        }
    }

    public static class KafkaStreamListener implements Runnable {

        private final Cluster cluster;
        private final KafkaStream<byte[], EventMessage> stream;
        private final TerminalMetricHelper metricHelper;
        private final TopicNormalizer topicNormalizer;

        public KafkaStreamListener(
                final Cluster cluster,
                final KafkaStream<byte[], EventMessage> stream,
                final TerminalMetricHelper metricHelper,
                final TopicNormalizer topicNormalizer
        ) {
            this.cluster = checkNotNull(cluster);
            this.stream = checkNotNull(stream);
            this.metricHelper= checkNotNull(metricHelper);
            this.topicNormalizer = checkNotNull(topicNormalizer);
        }

        @Override
        public void run() {
            final ConsumerIterator<byte[], EventMessage> it = stream.iterator();
            while (it.hasNext()) {
                final EventMessage message = it.next().message();

                final String topic = topicNormalizer.normalize(message.getPayloadType().getName());

                try {
                    cluster.publish(message);
                    LOGGER.debug("Received message with '{}' as identifier from the '{}' topic", message.getIdentifier(), topic);

                    metricHelper.markReceivedMessage(topic);
                } catch (Throwable t) {
                    LOGGER.error("Unexpected error while receiving a message with '{}' as identifier from the '{}' topic", message.getIdentifier(), topic, t);
                    metricHelper.markErroredWhileReceivingMessage(topic);
                }
            }
        }
    }
}
