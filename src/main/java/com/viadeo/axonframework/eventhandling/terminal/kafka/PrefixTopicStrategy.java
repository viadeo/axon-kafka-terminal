package com.viadeo.axonframework.eventhandling.terminal.kafka;

import com.google.common.base.Preconditions;
import org.axonframework.domain.EventMessage;

public class PrefixTopicStrategy extends DefaultTopicStrategy {

    private final String prefix;

    public PrefixTopicStrategy(final String prefix) {
        this.prefix = Preconditions.checkNotNull(prefix);
    }

    @Override
    public String getTopic(final EventMessage eventMessage) {
        return prefix + super.getTopic(eventMessage);
    }
}
