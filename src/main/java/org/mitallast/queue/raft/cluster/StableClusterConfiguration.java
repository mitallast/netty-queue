package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class StableClusterConfiguration implements ClusterConfiguration {
    private final ImmutableSet<DiscoveryNode> members;

    public StableClusterConfiguration(StreamInput stream) throws IOException {
        members = stream.readStreamableSet(DiscoveryNode::new);
    }

    public StableClusterConfiguration(DiscoveryNode... members) {
        this(ImmutableSet.copyOf(members));
    }

    public StableClusterConfiguration(ImmutableSet<DiscoveryNode> members) {
        this.members = members;
    }

    @Override
    public ImmutableSet<DiscoveryNode> members() {
        return members;
    }

    @Override
    public boolean isTransitioning() {
        return false;
    }

    @Override
    public ClusterConfiguration transitionTo(ClusterConfiguration newConfiguration) {
        return new JointConsensusClusterConfiguration(members, newConfiguration.members());
    }

    @Override
    public ClusterConfiguration transitionToStable() {
        return this;
    }

    @Override
    public boolean containsOnNewState(DiscoveryNode member) {
        return members.contains(member);
    }

    public StableClusterConfiguration withMember(DiscoveryNode member) {
        ImmutableSet<DiscoveryNode> newMembers = ImmutableSet.<DiscoveryNode>builder()
            .addAll(members)
            .add(member)
            .build();

        return new StableClusterConfiguration(newMembers);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamableSet(members);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StableClusterConfiguration that = (StableClusterConfiguration) o;

        return members.equals(that.members);

    }

    @Override
    public int hashCode() {
        return members.hashCode();
    }

    @Override
    public String toString() {
        return "StableClusterConfiguration{members=" + members + '}';
    }
}
