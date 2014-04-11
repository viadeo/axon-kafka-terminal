package com.viadeo.axonframework.eventhandling;

import com.google.common.collect.Lists;
import com.viadeo.axonframework.eventhandling.cluster.ClusterSelectorFactory;
import com.viadeo.axonframework.eventhandling.terminal.EventBusTerminalFactory;
import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.*;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class EventBusRule implements MethodRule {

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
    public Statement apply(final Statement base, final FrameworkMethod method, final Object target) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                eventBusWrapper = new EventBusWrapper(
                        clusterSelectorFactory.create(),
                        terminalFactory.create()
                );
                try {
                    base.evaluate();
                } finally {
                    shutdown();
                }
            }
        };
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
        eventBusWrapper.subscribe(eventListeners);
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
}
