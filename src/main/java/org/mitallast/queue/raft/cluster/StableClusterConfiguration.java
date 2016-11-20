package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class StableClusterConfiguration implements ClusterConfiguration {

    private final long sequenceNumber;
    private final ImmutableSet<DiscoveryNode> members;

    public StableClusterConfiguration(StreamInput stream) throws IOException {
        sequenceNumber = stream.readLong();
        members = stream.readStreamableSet(DiscoveryNode::new);
    }

    public StableClusterConfiguration(long sequenceNumber, DiscoveryNode... members) {
        this(sequenceNumber, ImmutableSet.copyOf(members));
    }

    public StableClusterConfiguration(long sequenceNumber, ImmutableSet<DiscoveryNode> members) {
        this.sequenceNumber = sequenceNumber;
        this.members = members;
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
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
        return false;
    }

    @Override
    public ClusterConfiguration transitionTo(ClusterConfiguration newConfiguration) {
        return new JointConsensusClusterConfiguration(sequenceNumber + 1, members, newConfiguration.members());
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

        return new StableClusterConfiguration(sequenceNumber, newMembers);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(sequenceNumber);
        stream.writeStreamableSet(members);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StableClusterConfiguration that = (StableClusterConfiguration) o;

        if (sequenceNumber != that.sequenceNumber) return false;
        return members.equals(that.members);

    }

    @Override
    public int hashCode() {
        int result = (int) (sequenceNumber ^ (sequenceNumber >>> 32));
        result = 31 * result + members.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "StableClusterConfiguration{" +
            "seq=" + sequenceNumber +
            ", members=" + members +
            '}';
    }
}
