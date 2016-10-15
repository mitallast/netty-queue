package org.mitallast.queue.raft2;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Optional;

public class StateMetadata {
    private final Term currentTerm;
    private final ClusterConfiguration config;
    private final Optional<DiscoveryNode> votedFor;
    private final int votesReceived;

    public StateMetadata() {
        this(new Term(0));
    }

    public StateMetadata(Term currentTerm) {
        this(currentTerm, new StableClusterConfiguration(0, ImmutableSet.of()));
    }

    public StateMetadata(Term currentTerm, ClusterConfiguration config) {
        this(currentTerm, config, Optional.empty());
    }

    public StateMetadata(Term currentTerm, ClusterConfiguration config, Optional<DiscoveryNode> votedFor) {
        this(currentTerm, config, votedFor, 0);
    }

    public StateMetadata(Term currentTerm, ClusterConfiguration config, Optional<DiscoveryNode> votedFor, int votesReceived) {
        this.currentTerm = currentTerm;
        this.config = config;
        this.votedFor = votedFor;
        this.votesReceived = votesReceived;
    }

    public Term getCurrentTerm() {
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
                .addAll(config.members().stream().filter(node -> node.equals(member)).iterator())
                .build();
    }

    public ImmutableSet<DiscoveryNode> members() {
        return config.members();
    }

    public boolean isConfigTransitionInProgress() {
        return config.isTransitioning();
    }

    public boolean canVoteIn(Term term) {
        return !votedFor.isPresent() && term.equals(currentTerm);
    }

    public boolean cannotVoteIn(Term term) {
        return term.less(currentTerm) || votedFor.isPresent();
    }

    public StateMetadata forNewElection() {
        return new StateMetadata(currentTerm.next(), config);
    }

    public StateMetadata withTerm(Term term) {
        return new StateMetadata(term, config, votedFor, votesReceived);
    }

    public StateMetadata incTerm() {
        return new StateMetadata(currentTerm.next(), config, votedFor, votesReceived);
    }

    public StateMetadata withVoteFor(DiscoveryNode candidate) {
        return new StateMetadata(currentTerm, config, Optional.of(candidate), votesReceived);
    }

    public StateMetadata withConfig(ClusterConfiguration config) {
        return new StateMetadata(currentTerm, config, votedFor, votesReceived);
    }

    public boolean hasMajority() {
        return votesReceived > config.members().size() / 2;
    }

    public StateMetadata incVote() {
        return new StateMetadata(currentTerm, config, votedFor, votesReceived + 1);
    }

    public StateMetadata forLeader() {
        return new StateMetadata(currentTerm, config);
    }

    public StateMetadata forFollower() {
        return new StateMetadata(currentTerm, config);
    }

    public StateMetadata forFollower(Term term) {
        return new StateMetadata(term, config);
    }
}
