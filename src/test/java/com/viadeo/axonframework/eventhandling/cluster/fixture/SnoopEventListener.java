package com.viadeo.axonframework.eventhandling.cluster.fixture;

import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.EventListener;
import org.junit.Assert;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;

public class SnoopEventListener implements EventListener {

    public final CountDownLatch countDownLatch;
    public final Queue<? extends EventMessage> expectedEvents;

    public SnoopEventListener(final CountDownLatch countDownLatch, final Queue<? extends EventMessage> expectedEvents) {
        this.expectedEvents = expectedEvents;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void handle(EventMessage eventMessage) {
        if(null != countDownLatch){
            if(0 == countDownLatch.getCount()){
                throw new AssertionError("unexpected event message : " + eventMessage);
            }
            countDownLatch.countDown();
            Assert.assertEquals(expectedEvents.poll(), eventMessage);
        }
    }
}
