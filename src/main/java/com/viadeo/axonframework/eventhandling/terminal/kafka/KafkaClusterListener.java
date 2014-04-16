package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.viadeo.axonframework.eventhandling.Shutdownable;
import kafka.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaClusterListener implements Shutdownable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterListener.class);

    private final ExecutorService executor;
    private final ConsumerConnector consumer;

    public KafkaClusterListener(
            final ConsumerFactory consumerFactory,
            final KafkaMetricHelper metricHelper,
            final TopicNormalizer topicNormalizer,
            final Cluster cluster,
            final int numberOfStreamListener
    ) {
        checkNotNull(consumerFactory);
        checkNotNull(metricHelper);
        checkNotNull(topicNormalizer);
        checkNotNull(cluster);

        checkArgument(numberOfStreamListener > 0, "The number of stream listener must be greater or equal to 1");

        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("event-consumer-" + cluster.getName() + "-%d").build();
        this.executor = Executors.newFixedThreadPool(numberOfStreamListener, threadFactory);

        this.consumer = consumerFactory.createConnector(cluster.getName());

        final List<KafkaStream<byte[], EventMessage>> streams = consumerFactory.createStreams(numberOfStreamListener, consumer);
        for (final KafkaStream<byte[], EventMessage> stream : streams) {
            executor.submit(new KafkaStreamListener(cluster, stream, metricHelper, topicNormalizer));
        }

        LOGGER.debug("Created cluster listener on '{}'", cluster.getName());
    }

    @Override
    public void shutdown() {
        executor.shutdown();

        consumer.commitOffsets();
        consumer.shutdown();
    }

    public static class KafkaStreamListener implements Runnable {

        private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamListener.class);

        private final Cluster cluster;
        private final KafkaStream<byte[], EventMessage> stream;
        private final KafkaMetricHelper metricHelper;
        private final TopicNormalizer topicNormalizer;

        public KafkaStreamListener(
                final Cluster cluster,
                final KafkaStream<byte[], EventMessage> stream,
                final KafkaMetricHelper metricHelper,
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

            LOGGER.debug("Running stream listener : {}", cluster.getName());

            while (it.hasNext()) {
                final EventMessage message = it.next().message();

                final String topic = topicNormalizer.normalize(message.getPayloadType().getName());

                try {
                    cluster.publish(message);
                    LOGGER.info("Received message with '{}' as identifier from the '{}' topic, payload:'{}'",
                            message.getIdentifier(), topic, message.getPayload());

                    metricHelper.markReceivedMessage(topic);
                } catch (Throwable t) {
                    LOGGER.error("Unexpected error while receiving a message with '{}' as identifier from the '{}' topic",
                            message.getIdentifier(), topic, t);
                    metricHelper.markErroredWhileReceivingMessage(topic);
                }
            }
        }
    }
}
