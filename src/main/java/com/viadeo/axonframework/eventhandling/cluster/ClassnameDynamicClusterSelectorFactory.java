package com.viadeo.axonframework.eventhandling.cluster;

import org.axonframework.eventhandling.ClusterSelector;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClassnameDynamicClusterSelectorFactory implements ClusterSelectorFactory {

    private final String prefix;
    private final ClusterFactory clusterFactory;

    public ClassnameDynamicClusterSelectorFactory(String prefix, ClusterFactory clusterFactory) {
        this.prefix = checkNotNull(prefix);
        this.clusterFactory = checkNotNull(clusterFactory);
    }

    @Override
    public ClusterSelector create() {
        return new ClassnameDynamicClusterSelector(prefix, clusterFactory);
    }
}
