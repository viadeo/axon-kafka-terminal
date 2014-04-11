package com.viadeo.axonframework.eventhandling.cluster;

import org.axonframework.eventhandling.ClusterSelector;

public interface ClusterSelectorFactory {
    ClusterSelector create();
}
