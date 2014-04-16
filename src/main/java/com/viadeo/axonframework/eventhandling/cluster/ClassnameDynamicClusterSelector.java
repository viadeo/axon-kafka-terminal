package com.viadeo.axonframework.eventhandling.cluster;

import org.axonframework.eventhandling.AbstractClusterSelector;
import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClassnameDynamicClusterSelector extends AbstractClusterSelector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClassnameDynamicClusterSelector.class);

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
        final String group;

        if ( ! matcher.matches()) {
            group = "default";
            LOGGER.error("No cluster name matched from package '{}' using regex '{}'. Using '{}' as default cluster name",
                    packageName, pattern, group);
        } else {
            group = matcher.group(1);
        }

        Cluster cluster = cache.get(group);

        if (null == cluster) {
            cluster = clusterFactory.create(group);
            cache.put(group, cluster);
        }

        return cluster;
    }
}
