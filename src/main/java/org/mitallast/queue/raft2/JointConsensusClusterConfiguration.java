package org.mitallast.queue.raft2;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.transport.DiscoveryNode;

public class JointConsensusClusterConfiguration implements ClusterConfiguration {

    private final long sequenceNumber;
    private final ImmutableSet<DiscoveryNode> oldMembers;
    private final ImmutableSet<DiscoveryNode> newMembers;
    private final ImmutableSet<DiscoveryNode> members;

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
                "Migrating from [" + oldMembers.size() + "] " + oldMembers + " to [" + newMembers.size() + "] " + newMembers );
    }

    @Override
    public ClusterConfiguration transitionToStable() {
        return new StableClusterConfiguration(sequenceNumber + 1, newMembers);
    }

    @Override
    public boolean containsOnNewState(DiscoveryNode member) {
        return newMembers.contains(member);
    }
}
