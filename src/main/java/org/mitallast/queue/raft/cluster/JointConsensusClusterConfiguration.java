package org.mitallast.queue.raft.cluster;

import javaslang.collection.Set;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.transport.DiscoveryNode;

public class JointConsensusClusterConfiguration implements ClusterConfiguration {
    public static final Codec<JointConsensusClusterConfiguration> codec = Codec.of(
        JointConsensusClusterConfiguration::new,
        JointConsensusClusterConfiguration::getOldMembers,
        JointConsensusClusterConfiguration::getNewMembers,
        Codec.setCodec(DiscoveryNode.codec),
        Codec.setCodec(DiscoveryNode.codec)
    );

    private final Set<DiscoveryNode> oldMembers;
    private final Set<DiscoveryNode> newMembers;
    private final Set<DiscoveryNode> members;

    public JointConsensusClusterConfiguration(Set<DiscoveryNode> oldMembers, Set<DiscoveryNode> newMembers) {
        this.oldMembers = oldMembers;
        this.newMembers = newMembers;
        this.members = oldMembers.addAll(newMembers);
    }

    public Set<DiscoveryNode> getOldMembers() {
        return oldMembers;
    }

    public Set<DiscoveryNode> getNewMembers() {
        return newMembers;
    }

    @Override
    public Set<DiscoveryNode> members() {
        return members;
    }

    @Override
    public boolean isTransitioning() {
        return true;
    }

    @Override
    public ClusterConfiguration transitionTo(ClusterConfiguration state) {
        throw new IllegalStateException("Cannot start another configuration transition, already in progress! " +
                "Migrating from [" + oldMembers.size() + "] " + oldMembers + " to [" + newMembers.size() + "] " + newMembers);
    }

    @Override
    public ClusterConfiguration transitionToStable() {
        return new StableClusterConfiguration(newMembers);
    }

    @Override
    public boolean containsOnNewState(DiscoveryNode member) {
        return newMembers.contains(member);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JointConsensusClusterConfiguration that = (JointConsensusClusterConfiguration) o;

        if (!oldMembers.equals(that.oldMembers)) return false;
        return newMembers.equals(that.newMembers);
    }

    @Override
    public int hashCode() {
        int result = oldMembers.hashCode();
        result = 31 * result + newMembers.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "JointConsensusClusterConfiguration{" +
            "old=" + oldMembers +
            ", new=" + newMembers +
            '}';
    }
}
