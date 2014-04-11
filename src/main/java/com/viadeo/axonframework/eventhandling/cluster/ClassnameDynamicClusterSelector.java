package com.viadeo.axonframework.eventhandling.cluster;

import org.axonframework.eventhandling.AbstractClusterSelector;
import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.EventListener;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ClassnameDynamicClusterSelector extends AbstractClusterSelector {

    private final Pattern pattern;
    private final HashMap<String, Cluster> cache;
    private final ClusterFactory clusterFactory;

    public ClassnameDynamicClusterSelector(final String prefix, final ClusterFactory clusterFactory) {
        final String escapedPrefix = checkNotNull(prefix).replace(".", "\\.");
        final String regex = String.format("%s\\.([^\\.]*).*", escapedPrefix);
        this.pattern = Pattern.compile(regex);
        this.cache = new HashMap<>();
        this.clusterFactory = checkNotNull(clusterFactory);
    }

    @Override
    protected Cluster doSelectCluster(EventListener eventListener, Class<?> listenerType) {
        final String packageName = listenerType.getPackage().getName();
        final Matcher matcher = pattern.matcher(packageName);
        checkArgument(matcher.matches(), "Unable to match domain name from package <%s> using regex <%s>", packageName, pattern);

        final String group = matcher.group(1);
        Cluster cluster = cache.get(group);

        if (null == cluster) {
            cluster = clusterFactory.create(group);
            cache.put(group, cluster);
        }

        return cluster;
    }
}
