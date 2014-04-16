package com.viadeo.axonframework.eventhandling.terminal.kafka;

import org.axonframework.domain.GenericEventMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DefaultTopicStrategyUTest {

    @Test
    public void normalize_withFullyQualifiedNameOfAClass_isOk() throws Exception {
        // Given
        final DefaultTopicStrategy strategy = new DefaultTopicStrategy();

        // When
        final String actualTopic = strategy.normalize("com.viadeo.kasper.client.platform.components.EventB");

        // Then
        assertNotNull(actualTopic);
        assertEquals("com.viadeo.kasper.client.platform.components.eventb", actualTopic);
    }

    @Test
    public void normalize_withFullyQualifiedNameOfAnInnerClass_isOk() throws Exception {
        // Given
        final DefaultTopicStrategy strategy = new DefaultTopicStrategy();

        // When
        final String actualTopic = strategy.normalize("com.viadeo.kasper.client.platform.components.DomainA$EventA");

        // Then
        assertNotNull(actualTopic);
        assertEquals("com.viadeo.kasper.client.platform.components.domaina_eventa", actualTopic);
    }

    @Test
    public void normalize_withAccentedCharacters_isOk() throws Exception {
        // Given
        final DefaultTopicStrategy strategy = new DefaultTopicStrategy();

        // When
        final String actualTopic = strategy.normalize("ĉoùcöu");

        // Then
        assertNotNull(actualTopic);
        assertEquals("coucou", actualTopic);
    }

    @Test
    public void normalize_withCharactersInUpperCase_isOk() throws Exception {
        // Given
        final DefaultTopicStrategy strategy = new DefaultTopicStrategy();

        // When
        final String actualTopic = strategy.normalize("CouCou");

        // Then
        assertNotNull(actualTopic);
        assertEquals("coucou", actualTopic);
    }

    @Test
    public void getTopic_fromEventMessage_shouldReturnPayloadClassName() {
        // Given
        final DefaultTopicStrategy strategy = new DefaultTopicStrategy();

        // When
        final String topic = strategy.getTopic(new GenericEventMessage<>("miam"));

        // Then
        assertEquals("java.lang.string", topic);
    }
}
