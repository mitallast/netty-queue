package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public interface ClusterConfiguration extends Streamable {

    ImmutableSet<DiscoveryNode> members();

    boolean isTransitioning();

    ClusterConfiguration transitionTo(ClusterConfiguration state);

    ClusterConfiguration transitionToStable();

    boolean containsOnNewState(DiscoveryNode member);
}
