package com.viadeo.axonframework.eventhandling.terminal.kafka;

/**
 * Created with IntelliJ IDEA.
 * User: cmurer
 * Date: 18/04/14
 * Time: 10:18
 * To change this template use File | Settings | File Templates.
 */
public interface TopicStrategyFactory {
    TopicStrategy create();
}
