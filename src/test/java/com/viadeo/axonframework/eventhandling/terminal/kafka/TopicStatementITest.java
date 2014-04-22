package com.viadeo.axonframework.eventhandling.terminal.kafka;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TopicStatementITest {

    private static TopicStatement topicStatement;

    @BeforeClass
    public static void setup() {
        topicStatement = new TopicStatement(KafkaTerminalITest.KAFKA_PROPERTIES_MAP.get("zookeeper.connect"));
    }

    @AfterClass
    public static void clear() {
        topicStatement.remove("A", "B", "C", "D");
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
