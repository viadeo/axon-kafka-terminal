package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
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
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class KafkaClusterListener implements Shutdownable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterListener.class);

    private final ExecutorService executor;
    private final ConsumerConnector consumer;
    private final List<KafkaStreamListener> streamListeners;
    private final Cluster cluster;

    public KafkaClusterListener(
            final ConsumerFactory consumerFactory,
            final KafkaMetricHelper metricHelper,
            final TopicStrategy topicStrategy,
            final Cluster cluster,
            final int numberOfStreamListener
    ) {
        checkNotNull(consumerFactory);
        checkNotNull(metricHelper);
        checkNotNull(topicStrategy);
        this.cluster = checkNotNull(cluster);

        checkArgument(numberOfStreamListener > 0, "The number of stream listener must be greater or equal to 1");

        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("event-consumer-" + cluster.getName() + "-%d").build();
        this.executor = Executors.newFixedThreadPool(numberOfStreamListener, threadFactory);

        this.streamListeners = Lists.newArrayList();

        this.consumer = consumerFactory.createConnector(cluster.getName());

        final List<KafkaStream<byte[], EventMessage>> streams = consumerFactory.createStreams(numberOfStreamListener, consumer);
        for (final KafkaStream<byte[], EventMessage> stream : streams) {
            KafkaStreamListener streamListener = new KafkaStreamListener(cluster, stream, metricHelper);
            streamListeners.add(streamListener);
            executor.submit(streamListener);
        }

        LOGGER.debug("Created cluster listener on '{}'", cluster.getName());
    }

    @Override
    public void shutdown() {
        final CountDownLatch countDownLatch = new CountDownLatch(streamListeners.size());
        final TerminateCallback terminateCallback = new TerminateCallback() {
            @Override
            public void terminated(final KafkaStreamListener streamListener) {
                LOGGER.debug("Terminated stream listener on '{}'", streamListener.cluster.getName());
                countDownLatch.countDown();
            }
        };

        for (final KafkaStreamListener streamListener:streamListeners) {
            streamListener.terminate(terminateCallback);
        }

        try {
            countDownLatch.await(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Unexpected error during terminate stream listener on '{}'", cluster.getName(), e);
        }

        executor.shutdownNow();

        consumer.commitOffsets();
        consumer.shutdown();
    }

    public static class KafkaStreamListener implements Runnable {

        private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamListener.class);

        private final Cluster cluster;
        private final KafkaStream<byte[], EventMessage> stream;
        private final KafkaMetricHelper metricHelper;

        private Optional<TerminateCallback> terminateCallback;

        public KafkaStreamListener(
                final Cluster cluster,
                final KafkaStream<byte[], EventMessage> stream,
                final KafkaMetricHelper metricHelper
        ) {
            this.cluster = checkNotNull(cluster);
            this.stream = checkNotNull(stream);
            this.metricHelper= checkNotNull(metricHelper);
            this.terminateCallback = Optional.absent();
        }

        @Override
        public void run() {
            final ConsumerIterator<byte[], EventMessage> it = stream.iterator();

            LOGGER.debug("Running stream listener : {}", cluster.getName());

            while (it.hasNext() && ! terminateCallback.isPresent()) {
                final EventMessage message = it.next().message();
                final String event = message.getPayloadType().getSimpleName();

                try {
                    cluster.publish(message);
                    LOGGER.info("Received '{}' message with '{}' as identifier", event, message.getIdentifier());

                    metricHelper.markReceivedMessage(event);
                } catch (Throwable t) {
                    LOGGER.error("Unexpected error while receiving '{}' message with '{}' as identifier", event,
                            message.getIdentifier(), t);
                    metricHelper.markErroredWhileReceivingMessage(event);
                }
            }

            terminateCallback.get().terminated(this);
        }

        public void terminate(final TerminateCallback terminateCallback) {
            checkState( ! this.terminateCallback.isPresent(), "The terminate action is already in progress!");
            this.terminateCallback = Optional.fromNullable(terminateCallback);
        }
    }

    public interface TerminateCallback {
        void terminated(final KafkaStreamListener streamListener);
    }
}
