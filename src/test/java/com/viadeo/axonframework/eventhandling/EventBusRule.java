package com.viadeo.axonframework.eventhandling;

import com.google.common.collect.Lists;
import com.viadeo.axonframework.eventhandling.cluster.ClassnameDynamicClusterSelectorFactory;
import com.viadeo.axonframework.eventhandling.cluster.ClusterFactory;
import com.viadeo.axonframework.eventhandling.cluster.ClusterSelectorFactory;
import com.viadeo.axonframework.eventhandling.terminal.EventBusTerminalFactory;
import com.viadeo.axonframework.eventhandling.terminal.kafka.KafkaTerminalFactory;
import com.viadeo.axonframework.eventhandling.terminal.kafka.PrefixTopicStrategy;
import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.*;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.viadeo.axonframework.eventhandling.terminal.kafka.KafkaTerminalFactory.from;

public class EventBusRule extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventBusRule.class);

    private final EventBusTerminalFactory terminalFactory;
    private final ClusterSelectorFactory clusterSelectorFactory;

    private EventBusWrapper eventBusWrapper;

    public EventBusRule(
            final EventBusTerminalFactory terminalFactory,
            final ClusterSelectorFactory clusterSelectorFactory
    ) {
        this.clusterSelectorFactory = clusterSelectorFactory;
        this.terminalFactory = terminalFactory;
    }

    @Override
    protected void before() throws Throwable {
//        final String prefix = UUID.randomUUID().toString();
        final String prefix = "";

        LOGGER.debug("Generated topic prefix : {}", prefix);

        eventBusWrapper = new EventBusWrapper(
                clusterSelectorFactory.create(),
                terminalFactory.with(new PrefixTopicStrategy(prefix)).create()
        );
    }

    @Override
    protected void after() {
        shutdown();
    }

    private void shutdown() {
        LOGGER.info("shutdowning event bus...");
        eventBusWrapper.shutdown();
    }

    public void publish(final EventMessage event) {
        LOGGER.info("publishing event bus...");
        eventBusWrapper.publish(event);
    }

    public void subscribe(final EventListener... eventListeners) {
        LOGGER.info("subscribing {} event listeners...", eventListeners.length);
        eventBusWrapper.subscribe(eventListeners);
    }

    public void unsubscribeAll() {
        eventBusWrapper.unsubscribeAll();
    }

    public static class EventBusWrapper implements Shutdownable {

        private final List<EventListener> eventListeners;
        private final EventBus eventBus;
        private final EventBusTerminal terminal;

        public EventBusWrapper(final ClusterSelector clusterSelector, final EventBusTerminal terminal) {
            this.terminal = terminal;
            this.eventBus = new ClusteringEventBus(clusterSelector, terminal);
            this.eventListeners = Lists.newArrayList();
        }

        public void publish(final EventMessage event) {
            eventBus.publish(event);
        }

        public void subscribe(final EventListener... eventListeners) {
            for (final EventListener eventListener : eventListeners) {
                eventBus.subscribe(eventListener);
                this.eventListeners.add(eventListener);
            }
        }

        public void unsubscribeAll() {
            for (final EventListener eventListener : eventListeners) {
                eventBus.unsubscribe(eventListener);
            }
            eventListeners.clear();
        }

        @Override
        public void shutdown() {
            unsubscribeAll();
            if (terminal instanceof Shutdownable) {
                try {
                    ((Shutdownable)terminal).shutdown();
                } catch (IOException e) {
                    LOGGER.error("Unexpected error while shutdown the terminal");
                }
            }
        }
    }

    public static ClusterSelectorFactory createClusterSelectorFactory(final String prefix) {
        return new ClassnameDynamicClusterSelectorFactory(
                prefix,
                new ClusterFactory() {
                    @Override
                    public Cluster create(final String name) {
                        return new SimpleCluster(name);
                    }
                }
        );
    }

    public static EventBusTerminalFactory createEventBusTerminalFactory(final Map<String, String> properties) {
        return new KafkaTerminalFactory(from(properties));
    }
}
