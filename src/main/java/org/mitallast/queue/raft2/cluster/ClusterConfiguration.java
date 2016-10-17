package org.mitallast.queue.raft2.cluster;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public interface ClusterConfiguration extends Streamable {

    long sequenceNumber();

    ImmutableSet<DiscoveryNode> members();

    int quorum();

    boolean isNewer(ClusterConfiguration state);

    boolean isTransitioning();

    ClusterConfiguration transitionTo(ClusterConfiguration state);

    ClusterConfiguration transitionToStable();

    boolean containsOnNewState(DiscoveryNode member);
}
