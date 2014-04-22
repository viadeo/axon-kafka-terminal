package com.viadeo.axonframework.eventhandling.cluster.fixture;

import com.google.common.collect.Lists;
import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class SnoopEventListener implements EventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnoopEventListener.class);

    public final CountDownLatch countDownLatch;
    public final List<EventMessage> actualEvents;

    public SnoopEventListener(final CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
        this.actualEvents = Lists.newArrayList();
    }

    @Override
    public void handle(EventMessage eventMessage) {
        if(null != countDownLatch){
            if(0 == countDownLatch.getCount()){
                throw new AssertionError("Unexpected event message : " + eventMessage);
            }
            actualEvents.add(eventMessage);

            LOGGER.debug("Received event message : {}", eventMessage);

            countDownLatch.countDown();
        }
    }
}
