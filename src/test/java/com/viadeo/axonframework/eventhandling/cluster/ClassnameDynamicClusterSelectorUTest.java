package com.viadeo.axonframework.eventhandling.cluster;

import com.viadeo.axonframework.eventhandling.cluster.fixture.groupb.GroupB;
import com.viadeo.axonframework.eventhandling.cluster.fixture.groupa.GroupA;
import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.SimpleCluster;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClassnameDynamicClusterSelectorUTest {

    public static final ClusterFactory CLUSTER_FACTORY = new ClusterFactory() {
        @Override
        public Cluster create(String name) {
            return new SimpleCluster(name);
        }
    };

    public static ClassnameDynamicClusterSelector initClusterSelector() throws Exception {
        return new ClassnameDynamicClusterSelector(
                "com.viadeo.axonframework.eventhandling.cluster.fixture",
                CLUSTER_FACTORY
        );
    }

    @Test(expected = NullPointerException.class)
    public void init_withNullAsGivenPrefix_throwException(){
        // Given nothing

        // When
        new ClassnameDynamicClusterSelector(null, CLUSTER_FACTORY);

        // Then throws an exception
    }

    @Test
    public void init_withPrefix_withFunction_isOk(){
        // Given nothing

        // When
        new ClassnameDynamicClusterSelector("", CLUSTER_FACTORY);

        // Then throws an exception
    }

    @Test(expected = NullPointerException.class)
    public void init_withNullAsGivenClusterFactory_throwException(){
        // Given nothing

        // When
        new ClassnameDynamicClusterSelector("", null);

        // Then throws an exception
    }

    @Test(expected = NullPointerException.class)
    public void selectCluster_withNullAsGivenEventListener_throwException() throws Exception {
        // Given
        final ClassnameDynamicClusterSelector clusterSelector = initClusterSelector();

        // When
        clusterSelector.selectCluster(null);

        // Then throws an exception
    }

    @Test
    public void selectCluster_withEventListenerB_returnClusterNamedAfterPackageOfListener() throws Exception {
        // Given
        final ClassnameDynamicClusterSelector clusterSelector = initClusterSelector();

        final GroupB.EventListenerB eventListenerB = new GroupB.EventListenerB();

        // When
        final Cluster cluster = clusterSelector.selectCluster(eventListenerB);

        // Then
        assertNotNull(cluster);
        assertEquals("groupb", cluster.getName());
    }

    @Test
    public void selectCluster_withEventListenerA_returnClusterNamedAfterPackageOfListener() throws Exception {
        // Given
        final ClassnameDynamicClusterSelector clusterSelector = initClusterSelector();

        final GroupA.EventListenerA eventListenerA = new GroupA.EventListenerA();

        // When
        final Cluster cluster = clusterSelector.selectCluster(eventListenerA);

        // Then
        assertNotNull(cluster);
        assertEquals("groupa", cluster.getName());
    }

    @Test
    public void selectCluster_withTwoEventListenersOfTheSameDomain_returnTheSameInstance() throws Exception {
        // Given
        final ClassnameDynamicClusterSelector clusterSelector = initClusterSelector();

        final GroupB.EventListenerB eventListenerB = new GroupB.EventListenerB();
        final GroupB.EventListenerA eventListenerA = new GroupB.EventListenerA();

        // When
        final Cluster cluster1 = clusterSelector.selectCluster(eventListenerB);
        final Cluster cluster2 = clusterSelector.selectCluster(eventListenerA);

        // Then
        assertNotNull(cluster1);
        assertNotNull(cluster2);
        assertSame(cluster1, cluster2);
    }
}
