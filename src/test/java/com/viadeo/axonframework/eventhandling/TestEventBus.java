package com.viadeo.axonframework.eventhandling;

import com.google.common.collect.Lists;
import com.viadeo.axonframework.eventhandling.cluster.ClusterSelectorFactory;
import com.viadeo.axonframework.eventhandling.terminal.kafka.*;
import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.ClusteringEventBus;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.EventBusTerminal;
import org.axonframework.eventhandling.EventListener;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TestEventBus extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestEventBus.class);

    private final KafkaTerminalFactory terminalFactory;
    private final ClusterSelectorFactory clusterSelectorFactory;
    private final TopicStrategyFactory topicStrategyFactory;
    private final TopicStatement topicStatement;
    private final List<EventListener> currentEventListeners;
    private final List<EventMessage> eventMessages;

    private TopicStrategy currentTopicStrategy;
    private EventBus currentEventBus;
    private EventBusTerminal currentTerminal;

    public TestEventBus(
            final KafkaTerminalFactory terminalFactory,
            final ClusterSelectorFactory clusterSelectorFactory,
            final String zkConnect
    ) {
        this(terminalFactory, clusterSelectorFactory, new TopicStrategyFactory() {
            @Override
            public TopicStrategy create() {
                final String prefix = UUID.randomUUID().toString();
                LOGGER.debug("Generated topic prefix : {}", prefix);

                // set the generated prefix as property, TODO found a better way in order to pass this restriction to our consumer
                terminalFactory.setConsumerProperty(ConsumerFactory.CONSUMER_TOPIC_FILTER_REGEX, prefix + ".*");

                return new PrefixTopicStrategy(prefix);
            }
        }, zkConnect);
    }

    public TestEventBus(
            final KafkaTerminalFactory terminalFactory,
            final ClusterSelectorFactory clusterSelectorFactory,
            final TopicStrategyFactory topicStrategyFactory,
            final String zkConnect
    ) {
        this.terminalFactory = checkNotNull(terminalFactory);
        this.clusterSelectorFactory = checkNotNull(clusterSelectorFactory);
        this.topicStrategyFactory = checkNotNull(topicStrategyFactory);

        this.eventMessages = Lists.newArrayList();
        this.currentEventListeners = Lists.newArrayList();
        this.topicStatement = new TopicStatement(checkNotNull(zkConnect));
    }

    public TestEventBus with(final EventMessage eventMessage) {
        this.eventMessages.add(eventMessage);
        return this;
    }

    @Override
    public void before() throws Throwable {
        currentTopicStrategy = topicStrategyFactory.create();

        for (final EventMessage eventMessage : eventMessages) {
            final String topic = currentTopicStrategy.getTopic(eventMessage);
            topicStatement.create(topic);
            LOGGER.debug("Created topic : {}", topic);
        }

        currentTerminal = terminalFactory.with(currentTopicStrategy).create();

        currentEventBus = new ClusteringEventBus(
                clusterSelectorFactory.create(),
                currentTerminal
        );
    }

    @Override
    public void after() {
        for (final EventListener eventListener : currentEventListeners) {
            currentEventBus.unsubscribe(eventListener);
        }
        currentEventListeners.clear();

        if (currentTerminal instanceof Closeable) {
            try {
                ((Closeable)currentTerminal).close();
            } catch (IOException e) {
                LOGGER.error("Unexpected error while shutdown the terminal");
            }
        }

        for (final EventMessage eventMessage : eventMessages) {
            final String topic = currentTopicStrategy.getTopic(eventMessage);
            topicStatement.remove(topic);
            LOGGER.debug("Deleted topic : {}", topic);
        }

        currentTopicStrategy = null;
    }

    public void publish(final EventMessage event) {
        checkState(currentEventBus != null, "Unable to publish : the event bus is undefined");
        LOGGER.debug("Publishing event message : {}",  event);
        currentEventBus.publish(event);
    }

    public void subscribe(final EventListener eventListener) {
        checkState(currentEventBus != null, "Unable to subscribe event listener : the event bus is undefined");
        LOGGER.debug("Subscribing event listener : {}",  eventListener);
        currentEventBus.subscribe(eventListener);
    }
}
