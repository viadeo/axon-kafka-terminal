package com.viadeo.axonframework.eventhandling.terminal.kafka;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TopicNormalizerUTest {

    @Test
    public void normalize_withFullyQualifiedNameOfAClass_isOk() throws Exception {
        // Given
        final TopicNormalizer normalizer = new TopicNormalizer();

        // When
        final String actualTopic = normalizer.normalize("com.viadeo.kasper.client.platform.components.EventB");

        // Then
        assertNotNull(actualTopic);
        assertEquals("com.viadeo.kasper.client.platform.components.EventB", actualTopic);
    }

    @Test
    public void normalize_withFullyQualifiedNameOfAnInnerClass_isOk() throws Exception {
        // Given
        final TopicNormalizer normalizer = new TopicNormalizer();

        // When
        final String actualTopic = normalizer.normalize("com.viadeo.kasper.client.platform.components.DomainA$EventA");

        // Then
        assertNotNull(actualTopic);
        assertEquals("com.viadeo.kasper.client.platform.components.DomainA_EventA", actualTopic);
    }

    @Test
    public void normalize_withAccentedCharacters_isOk() throws Exception {
        // Given
        final TopicNormalizer normalizer = new TopicNormalizer();

        // When
        final String actualTopic = normalizer.normalize("ĉoùcöu");

        // Then
        assertNotNull(actualTopic);
        assertEquals("coucou", actualTopic);
    }
}
