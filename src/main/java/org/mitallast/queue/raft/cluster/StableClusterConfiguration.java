package org.mitallast.queue.raft.cluster;

import javaslang.collection.HashSet;
import javaslang.collection.Set;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.transport.DiscoveryNode;

public class StableClusterConfiguration implements ClusterConfiguration {
    public static final Codec<StableClusterConfiguration> codec = Codec.Companion.of(
        StableClusterConfiguration::new,
        StableClusterConfiguration::members,
        Codec.Companion.setCodec(DiscoveryNode.codec)
    );

    private final Set<DiscoveryNode> members;

    public StableClusterConfiguration(DiscoveryNode... members) {
        this(HashSet.of(members));
    }

    public StableClusterConfiguration(Set<DiscoveryNode> members) {
        this.members = members;
    }

    @Override
    public Set<DiscoveryNode> members() {
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
