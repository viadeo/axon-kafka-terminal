package com.viadeo.axonframework.eventhandling.terminal;

import org.axonframework.eventhandling.EventBusTerminal;

public interface EventBusTerminalFactory {
    EventBusTerminal create();
}
