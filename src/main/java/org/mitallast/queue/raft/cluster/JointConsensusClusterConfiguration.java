package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class JointConsensusClusterConfiguration implements ClusterConfiguration {

    private final long sequenceNumber;
    private final ImmutableSet<DiscoveryNode> oldMembers;
    private final ImmutableSet<DiscoveryNode> newMembers;
    private final ImmutableSet<DiscoveryNode> members;

    public JointConsensusClusterConfiguration(StreamInput stream) throws IOException {
        sequenceNumber = stream.readLong();
        oldMembers = stream.readStreamableSet(DiscoveryNode::new);
        newMembers = stream.readStreamableSet(DiscoveryNode::new);
        members = ImmutableSet.<DiscoveryNode>builder().addAll(oldMembers).addAll(newMembers).build();
    }

    public JointConsensusClusterConfiguration(long sequenceNumber, ImmutableSet<DiscoveryNode> oldMembers, ImmutableSet<DiscoveryNode> newMembers) {
        this.sequenceNumber = sequenceNumber;
        this.oldMembers = oldMembers;
        this.newMembers = newMembers;
        this.members = ImmutableSet.<DiscoveryNode>builder().addAll(oldMembers).addAll(newMembers).build();
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
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
    public int quorum() {
        return members.size() / 2 + 1;
    }

    @Override
    public boolean isNewer(ClusterConfiguration state) {
        return sequenceNumber > state.sequenceNumber();
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
        return new StableClusterConfiguration(sequenceNumber + 1, newMembers);
    }

    @Override
    public boolean containsOnNewState(DiscoveryNode member) {
        return newMembers.contains(member);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(sequenceNumber);
        stream.writeStreamableSet(oldMembers);
        stream.writeStreamableSet(newMembers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JointConsensusClusterConfiguration that = (JointConsensusClusterConfiguration) o;

        if (sequenceNumber != that.sequenceNumber) return false;
        if (!oldMembers.equals(that.oldMembers)) return false;
        if (!newMembers.equals(that.newMembers)) return false;
        return members.equals(that.members);

    }

    @Override
    public int hashCode() {
        int result = (int) (sequenceNumber ^ (sequenceNumber >>> 32));
        result = 31 * result + oldMembers.hashCode();
        result = 31 * result + newMembers.hashCode();
        result = 31 * result + members.hashCode();
        return result;
    }
}
