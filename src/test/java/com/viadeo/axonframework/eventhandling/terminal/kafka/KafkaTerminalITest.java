package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.viadeo.axonframework.eventhandling.EventBusRule;
import com.viadeo.axonframework.eventhandling.cluster.ClassnameDynamicClusterSelectorFactory;
import com.viadeo.axonframework.eventhandling.cluster.ClusterFactory;
import com.viadeo.axonframework.eventhandling.cluster.fixture.SnoopEventListener;
import com.viadeo.axonframework.eventhandling.cluster.fixture.groupb.GroupB;
import com.viadeo.axonframework.eventhandling.cluster.fixture.groupa.GroupA;
import org.axonframework.domain.EventMessage;
import org.axonframework.domain.GenericEventMessage;
import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.EventListener;
import org.axonframework.eventhandling.SimpleCluster;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.viadeo.axonframework.eventhandling.terminal.kafka.KafkaTerminalFactory.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class KafkaTerminalITest {

    public static final ImmutableMap<String, String> KAFKA_PROPERTIES_MAP = ImmutableMap.<String, String>builder()
            .put("metadata.broker.list", "localhost:9092")
            .put("producer.type", "sync")
            .put("zookeeper.connect", "localhost:2181")
            .put("zookeeper.session.timeout.ms", "400")
            .put("zookeeper.sync.time.ms", "200")
            .put("group.id", "0")
            .put("auto.commit.interval.ms", "1000")
            .build();
    private static final long TIMEOUT = 2000L;

    @Rule
    public final EventBusRule eventBusRuleA = new EventBusRule(
            new KafkaTerminalFactory(from(KAFKA_PROPERTIES_MAP)),
            new ClassnameDynamicClusterSelectorFactory(
                    "com.viadeo.axonframework.eventhandling",
                    new ClusterFactory() {
                        @Override
                        public Cluster create(final String name) {
                            return new SimpleCluster(name);
                        }
                    }
            )
    );

    @Rule
    public final EventBusRule eventBusRuleB = new EventBusRule(
            new KafkaTerminalFactory(from(KAFKA_PROPERTIES_MAP)),
            new ClassnameDynamicClusterSelectorFactory(
                    "com.viadeo.axonframework.eventhandling",
                    new ClusterFactory() {
                        @Override
                        public Cluster create(final String name) {
                            return new SimpleCluster(name);
                        }
                    }
            )
    );

    @Test
    public void an_event_listener_should_receive_an_event_after_publication() throws InterruptedException {
        // Given
        final EventMessage eventMessage = new CustomEventMessage("A");

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        final EventListener eventListener = spy(new SnoopEventListener(countDownLatch, toQueue(eventMessage)));

        eventBusRuleA.subscribe(eventListener);

        // When
        eventBusRuleA.publish(eventMessage);
        countDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);

        // Then
        verify(eventListener).handle(any(EventMessage.class));
    }

    @Test
    public void an_event_listener_should_receive_an_ordered_sequence_of_events_after_publication() throws InterruptedException {
        // Given
        final List<CustomEventMessage> eventMessages = Lists.newArrayList(
                new CustomEventMessage("A"),
                new CustomEventMessage("B"),
                new CustomEventMessage("C")
        );

        final CountDownLatch countDownLatch = new CountDownLatch(eventMessages.size());

        final EventListener eventListener = spy(new SnoopEventListener(countDownLatch, toQueue(eventMessages)));

        eventBusRuleA.subscribe(eventListener);

        // When
        for (final CustomEventMessage eventMessage : eventMessages) {
            eventBusRuleA.publish(eventMessage);
        }
        countDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);

        // Then
        verify(eventListener, times(eventMessages.size())).handle(any(EventMessage.class));
    }

    @Test
    public void two_distinct_event_listeners_should_receive_the_event_after_publication() throws InterruptedException {
        // Given
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        final CustomEventMessage eventMessage = new CustomEventMessage("A");

        final GroupA.EventListenerA eventListenerAOfGroupA = spy(new GroupA.EventListenerA(new SnoopEventListener(countDownLatch, toQueue(eventMessage))));
        final GroupB.EventListenerA eventListenerAOfGroupB = spy(new GroupB.EventListenerA(new SnoopEventListener(countDownLatch, toQueue(eventMessage))));

        eventBusRuleA.subscribe(eventListenerAOfGroupA, eventListenerAOfGroupB);

        // When
        eventBusRuleA.publish(eventMessage);
        countDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);

        // Then
        verify(eventListenerAOfGroupA).handle(any(EventMessage.class));
        verify(eventListenerAOfGroupB).handle(any(EventMessage.class));
    }

    @Test
    public void two_event_listeners_defined_by_the_same_domain_should_receive_the_event_only_one_time_after_publication() throws InterruptedException {
        // Given
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        final CustomEventMessage eventMessage = new CustomEventMessage("A");

        final SnoopEventListener delegateEventListenerA = new SnoopEventListener(countDownLatch, toQueue(eventMessage));
        final GroupA.EventListenerA eventListenerA = new GroupA.EventListenerA(delegateEventListenerA);

        final SnoopEventListener delegateEventListenerB = new SnoopEventListener(countDownLatch, toQueue(eventMessage));
        final GroupA.EventListenerB eventListenerB = new GroupA.EventListenerB(delegateEventListenerB);

        eventBusRuleA.subscribe(eventListenerA);
        eventBusRuleB.subscribe(eventListenerB);

        // When
        eventBusRuleA.publish(eventMessage);
        countDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);

        // Then

        assertEquals(1, countDownLatch.getCount());
        assertTrue(
                (0 == delegateEventListenerA.expectedEvents.size() && 1 == delegateEventListenerB.expectedEvents.size()) ||
                        (1 == delegateEventListenerA.expectedEvents.size() && 0 == delegateEventListenerB.expectedEvents.size())
        );
    }

    public Queue<EventMessage> toQueue(final EventMessage... eventMessages) {
        return this.toQueue(Arrays.asList(eventMessages));
    }

    public <E extends EventMessage> Queue<E> toQueue(final List<E> eventMessages) {
        return Queues.newArrayDeque(eventMessages);
    }

    public static class CustomEventMessage extends GenericEventMessage<String> {
        public CustomEventMessage(String payload) {
            super(payload);
        }
    }

}
