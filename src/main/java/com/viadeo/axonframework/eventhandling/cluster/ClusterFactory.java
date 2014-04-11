package com.viadeo.axonframework.eventhandling.cluster;

import org.axonframework.eventhandling.Cluster;

public interface ClusterFactory {
    Cluster create(final String name);
}
