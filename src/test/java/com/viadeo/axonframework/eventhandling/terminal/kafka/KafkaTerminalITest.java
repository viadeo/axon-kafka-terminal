package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.viadeo.axonframework.eventhandling.TestEventBus;
import com.viadeo.axonframework.eventhandling.cluster.ClassnameDynamicClusterSelectorFactory;
import com.viadeo.axonframework.eventhandling.cluster.ClusterFactory;
import com.viadeo.axonframework.eventhandling.cluster.ClusterSelectorFactory;
import com.viadeo.axonframework.eventhandling.cluster.fixture.SnoopEventListener;
import com.viadeo.axonframework.eventhandling.cluster.fixture.groupa.GroupA;
import com.viadeo.axonframework.eventhandling.cluster.fixture.groupb.GroupB;
import org.axonframework.domain.EventMessage;
import org.axonframework.domain.GenericEventMessage;
import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.SimpleCluster;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.viadeo.axonframework.eventhandling.terminal.kafka.KafkaTerminalFactory.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class KafkaTerminalITest {

    private static final long TIMEOUT = 8000L;

    private static final String PREFIX = "com.viadeo.axonframework.eventhandling.cluster.fixture";

    public static final ImmutableMap<String, String> KAFKA_PROPERTIES_MAP = ImmutableMap.<String, String>builder()
            // PRODUCER
            .put("metadata.broker.list", "localhost:9092")
            .put("request.required.acks", "1")
            .put("producer.type", "sync")

            //CONSUMER
            .put("zookeeper.connect", "localhost:2181")
             // this property will be overridden by the cluster
            .put("group.id", "0")
            // !important; without the following property then this suite is unstable (due to the process of the auto creation topic)
            .put("auto.offset.reset", "smallest")

            .build();

    @Rule
    public final TestEventBus eventBusRule = new TestEventBus(
            createKafkaTerminalFactory(KAFKA_PROPERTIES_MAP),
            createClusterSelectorFactory(PREFIX),
            KAFKA_PROPERTIES_MAP.get("zookeeper.connect")
    ).with(new GenericEventMessage<>(""));


    @Test
    public void an_event_listener_should_receive_an_event_after_publication() throws InterruptedException {
        // Given
        final EventMessage eventMessage = new CustomEventMessage("A0");

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        final SnoopEventListener delegateEventListener = new SnoopEventListener(countDownLatch);
        final GroupA.EventListenerA eventListener = spy(new GroupA.EventListenerA(delegateEventListener));

        eventBusRule.subscribe(eventListener);

        // When
        eventBusRule.publish(eventMessage);
        countDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);

        // Then
        assertEquals("the number of received and expected message not matched,", 0, countDownLatch.getCount());

        verify(eventListener).handle(any(EventMessage.class));
        assertEquals(1, delegateEventListener.actualEvents.size());
        assertEquals("A0", delegateEventListener.actualEvents.get(0).getPayload());
    }

    @Test
    public void an_event_listener_should_receive_an_ordered_sequence_of_events_after_publication() throws InterruptedException {
        // Given
        final List<CustomEventMessage> eventMessages = Lists.newArrayList(
                new CustomEventMessage("A1"),
                new CustomEventMessage("B1"),
                new CustomEventMessage("C1")
        );

        final CountDownLatch countDownLatch = new CountDownLatch(eventMessages.size());

        final SnoopEventListener delegateEventListener = new SnoopEventListener(countDownLatch);
        final GroupA.EventListenerA eventListener = spy(new GroupA.EventListenerA(delegateEventListener));

        eventBusRule.subscribe(eventListener);

        // When
        for (final CustomEventMessage eventMessage : eventMessages) {
            eventBusRule.publish(eventMessage);
        }
        countDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);

        // Then
        assertEquals("the number of received and expected message not matched,", 0, countDownLatch.getCount());

        verify(eventListener, times(eventMessages.size())).handle(any(EventMessage.class));

        assertEquals(3, delegateEventListener.actualEvents.size());
        assertEquals("A1", delegateEventListener.actualEvents.get(0).getPayload());
        assertEquals("B1", delegateEventListener.actualEvents.get(1).getPayload());
        assertEquals("C1", delegateEventListener.actualEvents.get(2).getPayload());
    }

    @Test
    public void two_distinct_event_listeners_should_receive_the_event_after_publication() throws InterruptedException {
        // Given
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        final CustomEventMessage eventMessage = new CustomEventMessage("A2");

        final SnoopEventListener delegateEventListenerAOfGroupA = new SnoopEventListener(countDownLatch);
        final GroupA.EventListenerA eventListenerAOfGroupA = spy(new GroupA.EventListenerA(delegateEventListenerAOfGroupA));
        eventBusRule.subscribe(eventListenerAOfGroupA);

        final SnoopEventListener delegateEventListenerAOfGroupB = new SnoopEventListener(countDownLatch);
        final GroupB.EventListenerA eventListenerAOfGroupB = spy(new GroupB.EventListenerA(delegateEventListenerAOfGroupB));
        eventBusRule.subscribe(eventListenerAOfGroupB);

        // When
        eventBusRule.publish(eventMessage);
        countDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);

        // Then
        assertEquals("the number of received and expected message not matched,", 0, countDownLatch.getCount());

        verify(eventListenerAOfGroupA).handle(any(EventMessage.class));
        assertEquals("A2", delegateEventListenerAOfGroupA.actualEvents.get(0).getPayload());

        verify(eventListenerAOfGroupB).handle(any(EventMessage.class));
        assertEquals("A2", delegateEventListenerAOfGroupB.actualEvents.get(0).getPayload());
    }

    @Test
    public void two_event_listeners_defined_by_the_same_domain_should_receive_the_event_only_one_time_after_publication() throws Throwable {
        // Given
        final TestEventBus eventBusB = new TestEventBus(
                createKafkaTerminalFactory(KAFKA_PROPERTIES_MAP),
                createClusterSelectorFactory(PREFIX),
                KAFKA_PROPERTIES_MAP.get("zookeeper.connect")
        );
        eventBusB.before();

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        final SnoopEventListener delegateEventListenerA = new SnoopEventListener(countDownLatch);
        final GroupA.EventListenerA eventListenerA = new GroupA.EventListenerA(delegateEventListenerA);
        eventBusRule.subscribe(eventListenerA);

        final SnoopEventListener delegateEventListenerB = new SnoopEventListener(countDownLatch);
        final GroupA.EventListenerB eventListenerB = new GroupA.EventListenerB(delegateEventListenerB);
        eventBusB.subscribe(eventListenerB);

        final CustomEventMessage eventMessage = new CustomEventMessage("A4");

        // When
        eventBusRule.publish(eventMessage);
        countDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);

        // Then
        assertEquals("Unexpected number of received message", 0, countDownLatch.getCount());
        assertTrue("At least one of the subscribed listeners should receive the message",
                (0 == delegateEventListenerA.actualEvents.size() && 1 == delegateEventListenerB.actualEvents.size()) ||
                        (1 == delegateEventListenerA.actualEvents.size() && 0 == delegateEventListenerB.actualEvents.size())
        );

        // clean
        eventBusB.after();
    }

    public static class CustomEventMessage extends GenericEventMessage<String> {
        public CustomEventMessage(String payload) {
            super(payload);
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

    public static KafkaTerminalFactory createKafkaTerminalFactory(final Map<String, String> properties) {
        return new KafkaTerminalFactory(from(properties));
    }
}
