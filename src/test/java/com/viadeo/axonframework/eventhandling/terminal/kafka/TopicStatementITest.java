package com.viadeo.axonframework.eventhandling.terminal.kafka;

import org.junit.Before;
import org.junit.Test;

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
        topicStatement.create("TOTO");

        // Then
        assertTrue(topicStatement.getTopics().contains("TOTO"));
    }

    @Test
    public void prepare_withExistingTopic_isOk() {
        // Given
        topicStatement.create("TOTO");

        // When
        topicStatement.create("TOTO");

        // Then
        assertTrue(topicStatement.getTopics().contains("TOTO"));
    }

    @Test
    public void remove_withExistingTopic_isOk() {
        // Given
        topicStatement.create("TOTO");

        // When
        topicStatement.remove("TOTO");

        // Then
        assertFalse(topicStatement.getTopics().contains("TOTO"));
    }

    @Test
    public void remove_withNonexistentTopic_isOk() {
        // Given

        // When
        topicStatement.remove("FB");

        // Then
        assertFalse(topicStatement.getTopics().contains("FB"));
    }

}
