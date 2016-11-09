package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Optional;

public class RaftMetadata {
    private final Term currentTerm;
    private final ClusterConfiguration config;
    private final Optional<DiscoveryNode> votedFor;
    private final int votesReceived;

    public RaftMetadata() {
        this(new Term(0));
    }

    public RaftMetadata(Term currentTerm) {
        this(currentTerm, new StableClusterConfiguration(0, ImmutableSet.of()));
    }

    public RaftMetadata(Term currentTerm, ClusterConfiguration config) {
        this(currentTerm, config, Optional.empty());
    }

    public RaftMetadata(Term currentTerm, ClusterConfiguration config, Optional<DiscoveryNode> votedFor) {
        this(currentTerm, config, votedFor, 0);
    }

    public RaftMetadata(Term currentTerm, ClusterConfiguration config, Optional<DiscoveryNode> votedFor, int votesReceived) {
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
                .addAll(config.members().stream().filter(node -> !node.equals(member)).iterator())
                .build();
    }

    public ImmutableSet<DiscoveryNode> members() {
        return config.members();
    }

    public boolean canVoteIn(Term term) {
        return !votedFor.isPresent() && term.equals(currentTerm);
    }

    public RaftMetadata forNewElection() {
        return new RaftMetadata(currentTerm.next(), config);
    }

    public RaftMetadata withTerm(Term term) {
        return new RaftMetadata(term, config, votedFor, votesReceived);
    }

    public RaftMetadata withTerm(Optional<Term> term) {
        if(term.isPresent()){
            return new RaftMetadata(term.get(), config, votedFor, votesReceived);
        }else {
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
        return votesReceived > config.members().size() / 2;
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
}
