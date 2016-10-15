package org.mitallast.queue.raft2;

import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Optional;

public interface DomainEvent {
    class UpdateTermEvent implements DomainEvent {
        private final Term term;

        public UpdateTermEvent(Term term) {
            this.term = term;
        }

        public Term getTerm() {
            return term;
        }
    }

    class VoteForEvent implements DomainEvent {
        private final DiscoveryNode voteFor;

        public VoteForEvent(DiscoveryNode voteFor) {
            this.voteFor = voteFor;
        }

        public DiscoveryNode getVoteFor() {
            return voteFor;
        }
    }

    class VoteForSelfEvent implements DomainEvent {
    }

    class IncrementVoteEvent implements DomainEvent {
    }

    class GoToFollowerEvent implements DomainEvent {
        private final Optional<Term> term;

        public GoToFollowerEvent(Optional<Term> term) {
            this.term = term;
        }

        public Optional<Term> getTerm() {
            return term;
        }
    }

    class WithNewConfigEvent implements DomainEvent {
        private final Optional<Term> term;
        private final ClusterConfiguration config;

        public WithNewConfigEvent(Optional<Term> term, ClusterConfiguration config) {
            this.term = term;
            this.config = config;
        }

        public Optional<Term> getTerm() {
            return term;
        }

        public ClusterConfiguration getConfig() {
            return config;
        }
    }

    class GoToLeaderEvent implements DomainEvent {
    }

    class StartElectionEvent implements DomainEvent {
    }

    class KeepStateEvent implements DomainEvent {
    }
}
