package org.mitallast.queue.raft

import io.vavr.collection.Set
import io.vavr.control.Option
import org.mitallast.queue.raft.cluster.ClusterConfiguration
import org.mitallast.queue.raft.cluster.StableClusterConfiguration
import org.mitallast.queue.transport.DiscoveryNode

data class RaftMetadata(
        val currentTerm: Long = 0,
        val config: ClusterConfiguration = StableClusterConfiguration(),
        val votedFor: Option<DiscoveryNode> = Option.none(),
        val votesReceived: Int = 0
) {

    fun membersWithout(member: DiscoveryNode): Set<DiscoveryNode> {
        return config.members.remove(member)
    }

    fun members(): Set<DiscoveryNode> = config.members

    fun canVoteIn(term: Long) = votedFor.isEmpty && term == currentTerm

    fun forNewElection() = copy(currentTerm = currentTerm + 1, votedFor = Option.none(), votesReceived = 0)

    fun forFollower() = copy(votedFor = Option.none(), votesReceived = 0)

    fun withTerm(term: Long) = copy(currentTerm = term)

    fun withTerm(term: Option<Long>): RaftMetadata {
        return if (term.forAll { newTerm -> newTerm > currentTerm }) {
            copy(currentTerm=term.get())
        } else {
            this
        }
    }

    fun withVoteFor(candidate: DiscoveryNode) = copy(votedFor = Option.some(candidate))

    fun withConfig(config: ClusterConfiguration) = copy(config = config)

    fun hasMajority() = votesReceived > config.members.size() / 2

    fun incVote() = copy(votesReceived = votesReceived + 1)
}