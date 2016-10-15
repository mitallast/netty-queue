package org.mitallast.queue.raft2;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.transport.DiscoveryNode;

public interface ClusterConfiguration {

    long sequenceNumber();

    ImmutableSet<DiscoveryNode> members();

    int quorum();

    boolean isNewer(ClusterConfiguration state);

    boolean isTransitioning();

    ClusterConfiguration transitionTo(ClusterConfiguration state);

    ClusterConfiguration transitionToStable();

    boolean containsOnNewState(DiscoveryNode member);
}
