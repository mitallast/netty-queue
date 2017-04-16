package org.mitallast.queue.raft;

import javaslang.collection.Set;
import javaslang.control.Option;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.transport.DiscoveryNode;

public class RaftMetadata {
    /**
     * latest term server has seen (initialized to 0 on first boot, increases monotonically)
     */
    private final long currentTerm;
    private final ClusterConfiguration config;
    /**
     * candidateId that received vote in current term (or null if none)
     */
    private final Option<DiscoveryNode> votedFor;
    private final int votesReceived;

    public RaftMetadata(long currentTerm, ClusterConfiguration config) {
        this(currentTerm, config, Option.none());
    }

    public RaftMetadata(long currentTerm, ClusterConfiguration config, Option<DiscoveryNode> votedFor) {
        this(currentTerm, config, votedFor, 0);
    }

    public RaftMetadata(long currentTerm, ClusterConfiguration config, Option<DiscoveryNode> votedFor, int votesReceived) {
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

    public Option<DiscoveryNode> getVotedFor() {
        return votedFor;
    }

    public int getVotesReceived() {
        return votesReceived;
    }

    public Set<DiscoveryNode> membersWithout(DiscoveryNode member) {
        return config.members().remove(member);
    }

    public Set<DiscoveryNode> members() {
        return config.members();
    }

    public boolean canVoteIn(long term) {
        return votedFor.isEmpty() && term == currentTerm;
    }

    public RaftMetadata forNewElection() {
        return new RaftMetadata(currentTerm + 1, config);
    }

    public RaftMetadata forFollower() {
        return new RaftMetadata(currentTerm, config);
    }

    public RaftMetadata withTerm(long term) {
        return new RaftMetadata(term, config, votedFor, votesReceived);
    }

    public RaftMetadata withTerm(Option<Long> term) {
        if (term.forAll(newTerm -> newTerm > currentTerm)) {
            return new RaftMetadata(term.get(), config, votedFor, votesReceived);
        } else {
            return this;
        }
    }

    public RaftMetadata withVoteFor(DiscoveryNode candidate) {
        return new RaftMetadata(currentTerm, config, Option.some(candidate), votesReceived);
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
}
