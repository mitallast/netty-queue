package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.proto.raft.ClusterConfiguration;
import org.mitallast.queue.proto.raft.DiscoveryNode;
import org.mitallast.queue.proto.raft.JointConsensusClusterConfiguration;
import org.mitallast.queue.proto.raft.StableClusterConfiguration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RaftMetadata {
    /**
     * latest term server has seen (initialized to 0 on first boot, increases monotonically)
     */
    private final long currentTerm;
    private final ClusterConfiguration config;
    /**
     * candidateId that received vote in current term (or null if none)
     */
    private final Optional<DiscoveryNode> votedFor;
    private final int votesReceived;

    public RaftMetadata(long currentTerm, ClusterConfiguration config) {
        this(currentTerm, config, Optional.empty());
    }

    public RaftMetadata(long currentTerm, ClusterConfiguration config, Optional<DiscoveryNode> votedFor) {
        this(currentTerm, config, votedFor, 0);
    }

    public RaftMetadata(long currentTerm, ClusterConfiguration config, Optional<DiscoveryNode> votedFor, int votesReceived) {
        this.currentTerm = currentTerm;
        this.config = config;
        this.votedFor = votedFor;
        this.votesReceived = votesReceived;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public ClusterConfiguration getConfig() {
        return config;
    }

    public Optional<DiscoveryNode> getVotedFor() {
        return votedFor;
    }

    public int getVotesReceived() {
        return votesReceived;
    }

    public ImmutableSet<DiscoveryNode> membersWithout(DiscoveryNode member) {
        return ImmutableSet.<DiscoveryNode>builder()
            .addAll(members().stream().filter(node -> !node.equals(member)).iterator())
            .build();
    }

    public boolean isTransitioning() {
        return config.getConfigCase() == ClusterConfiguration.ConfigCase.JOINT;
    }

    public ImmutableSet<DiscoveryNode> members() {
        if (isTransitioning()) {
            return ImmutableSet.copyOf(config.getJoint().getMembersList());
        } else {
            return ImmutableSet.copyOf(config.getStable().getMembersList());
        }
    }

    public JointConsensusClusterConfiguration transitionTo(StableClusterConfiguration newConfig) {
        List<DiscoveryNode> members = Stream.concat(
            config.getStable().getMembersList().stream(),
            newConfig.getMembersList().stream()
        ).distinct().collect(Collectors.toList());

        return JointConsensusClusterConfiguration.newBuilder()
            .addAllOldMembers(config.getStable().getMembersList())
            .addAllNewMembers(newConfig.getMembersList())
            .addAllMembers(members)
            .build();
    }

    public boolean canVoteIn(long term) {
        return !votedFor.isPresent() && term == currentTerm;
    }

    public RaftMetadata forNewElection() {
        return new RaftMetadata(currentTerm + 1, config);
    }

    public RaftMetadata withTerm(long term) {
        return new RaftMetadata(term, config, votedFor, votesReceived);
    }

    public RaftMetadata withTerm(Optional<Long> term) {
        if (term.filter(newTerm -> newTerm > currentTerm).isPresent()) {
            return new RaftMetadata(term.get(), config, votedFor, votesReceived);
        } else {
            return this;
        }
    }

    public RaftMetadata withVoteFor(DiscoveryNode candidate) {
        return new RaftMetadata(currentTerm, config, Optional.of(candidate), votesReceived);
    }

    public RaftMetadata withConfig(ClusterConfiguration config) {
        return new RaftMetadata(currentTerm, config, votedFor, votesReceived);
    }

    public boolean hasMajority() {
        return votesReceived > members().size() / 2;
    }

    public RaftMetadata incVote() {
        return new RaftMetadata(currentTerm, config, votedFor, votesReceived + 1);
    }

    public RaftMetadata forLeader() {
        return new RaftMetadata(currentTerm, config);
    }

    public RaftMetadata forFollower() {
        return new RaftMetadata(currentTerm, config);
    }

    public boolean containsOnNewState(DiscoveryNode self) {
        if (isTransitioning()) {
            return config.getJoint().getNewMembersList().contains(self);
        } else {
            return config.getStable().getMembersList().contains(self);
        }
    }
}
