package org.mitallast.queue.raft.cluster;

import javaslang.collection.Set;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.transport.DiscoveryNode;

public class JointConsensusClusterConfiguration implements ClusterConfiguration {
    private final Set<DiscoveryNode> oldMembers;
    private final Set<DiscoveryNode> newMembers;
    private final Set<DiscoveryNode> members;

    public JointConsensusClusterConfiguration(StreamInput stream) {
        oldMembers = stream.readSet(DiscoveryNode::new);
        newMembers = stream.readSet(DiscoveryNode::new);
        members = oldMembers.addAll(newMembers);
    }

    public JointConsensusClusterConfiguration(Set<DiscoveryNode> oldMembers, Set<DiscoveryNode> newMembers) {
        this.oldMembers = oldMembers;
        this.newMembers = newMembers;
        this.members = oldMembers.addAll(newMembers);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeSet(oldMembers);
        stream.writeSet(newMembers);
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
        if (!newMembers.equals(that.newMembers)) return false;
        return members.equals(that.members);

    }

    @Override
    public int hashCode() {
        int result = oldMembers.hashCode();
        result = 31 * result + newMembers.hashCode();
        result = 31 * result + members.hashCode();
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
