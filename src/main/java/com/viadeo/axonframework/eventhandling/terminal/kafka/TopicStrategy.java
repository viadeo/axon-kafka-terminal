package com.viadeo.axonframework.eventhandling.terminal.kafka;

import org.axonframework.domain.EventMessage;

public interface TopicStrategy {
    String getTopic(final EventMessage eventMessage);
}
