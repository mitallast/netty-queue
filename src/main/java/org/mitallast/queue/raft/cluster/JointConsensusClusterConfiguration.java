package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class JointConsensusClusterConfiguration implements ClusterConfiguration {
    private final ImmutableSet<DiscoveryNode> oldMembers;
    private final ImmutableSet<DiscoveryNode> newMembers;
    private final ImmutableSet<DiscoveryNode> members;

    public JointConsensusClusterConfiguration(StreamInput stream) throws IOException {
        oldMembers = stream.readStreamableSet(DiscoveryNode::new);
        newMembers = stream.readStreamableSet(DiscoveryNode::new);
        members = ImmutableSet.<DiscoveryNode>builder().addAll(oldMembers).addAll(newMembers).build();
    }

    public JointConsensusClusterConfiguration(ImmutableSet<DiscoveryNode> oldMembers, ImmutableSet<DiscoveryNode> newMembers) {
        this.oldMembers = oldMembers;
        this.newMembers = newMembers;
        this.members = ImmutableSet.<DiscoveryNode>builder().addAll(oldMembers).addAll(newMembers).build();
    }

    public ImmutableSet<DiscoveryNode> getOldMembers() {
        return oldMembers;
    }

    public ImmutableSet<DiscoveryNode> getNewMembers() {
        return newMembers;
    }

    @Override
    public ImmutableSet<DiscoveryNode> members() {
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
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamableSet(oldMembers);
        stream.writeStreamableSet(newMembers);
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
