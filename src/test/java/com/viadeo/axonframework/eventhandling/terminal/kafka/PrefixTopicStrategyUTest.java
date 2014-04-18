package com.viadeo.axonframework.eventhandling.terminal.kafka;

import org.axonframework.domain.GenericEventMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrefixTopicStrategyUTest {

    @Test
    public void getTopic_returnPrefixedTopic() {
        // Given
        final PrefixTopicStrategy topicStrategy = new PrefixTopicStrategy("aloha_");

        // When
        final String topic = topicStrategy.getTopic(new GenericEventMessage<>("something"));

        // Then
        assertEquals("aloha_java.lang.string", topic);
    }
}
