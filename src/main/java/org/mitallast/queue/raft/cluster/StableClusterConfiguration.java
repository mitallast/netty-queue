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
        return new JointConsensusClusterConfiguration(sequenceNumber, members, newConfiguration.members());
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
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(sequenceNumber);
        stream.writeStreamableSet(members);
    }
}
