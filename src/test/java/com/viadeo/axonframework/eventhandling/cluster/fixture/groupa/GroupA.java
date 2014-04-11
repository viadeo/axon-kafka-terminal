package com.viadeo.axonframework.eventhandling.cluster.fixture.groupa;

import com.viadeo.axonframework.eventhandling.cluster.fixture.EventListenerWrapper;
import org.axonframework.eventhandling.EventListener;

public interface GroupA {

    class EventListenerA extends EventListenerWrapper {
        public EventListenerA() {
            this(null);
        }

        public EventListenerA(final EventListener delegate) {
            super(delegate);
        }
    }

    class EventListenerB extends EventListenerWrapper {
        public EventListenerB() {
            this(null);
        }

        public EventListenerB(final EventListener delegate) {
            super(delegate);
        }
    }
}
