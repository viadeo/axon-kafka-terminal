package com.viadeo.axonframework.eventhandling.terminal;

import com.viadeo.axonframework.eventhandling.terminal.kafka.TopicStrategy;
import org.axonframework.eventhandling.EventBusTerminal;

public interface EventBusTerminalFactory {
    EventBusTerminal create();

    EventBusTerminalFactory with(TopicStrategy topicStrategy);

    void setConsumerProperty(String consumerStreamFilterRegex, Object value);

    void setProducerProperty(String consumerStreamFilterRegex, Object value);

}
