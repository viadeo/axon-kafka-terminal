package com.viadeo.axonframework.eventhandling.terminal.kafka;

import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TopicStatementITest {

    private TopicStatement topicStatement;

    @Before
    public void setup() {
        topicStatement = new TopicStatement(KafkaTerminalITest.KAFKA_PROPERTIES_MAP.get("zookeeper.connect"));
    }

    @Test
    public void prepare_withTopic_isOk() {
        // Given

        // When
        topicStatement.create("A");

        // Then
        assertTrue(topicStatement.getTopics().contains("A"));
    }

    @Test
    public void prepare_withExistingTopic_isOk() {
        // Given
        topicStatement.create("B");

        // When
        topicStatement.create("B");

        // Then
        assertTrue(topicStatement.getTopics().contains("B"));
    }

    @Test
    public void remove_withExistingTopic_isOk() throws InterruptedException {
        // Given
        final String topic = UUID.randomUUID().toString();

        // When
        topicStatement.remove(topic);

        // Then
        assertFalse(topicStatement.getTopics().contains("TOTO"));
    }

    @Test
    public void remove_withNonexistentTopic_isOk() {
        // Given

        // When
        topicStatement.remove("D");

        // Then
        assertFalse(topicStatement.getTopics().contains("D"));
    }

}
