package org.mitallast.queue.raft.domain;

import org.mitallast.queue.transport.DiscoveryNode;

public class VoteForEvent implements DomainEvent {
    private final DiscoveryNode candidate;

    public VoteForEvent(DiscoveryNode candidate) {
        this.candidate = candidate;
    }

    public DiscoveryNode getCandidate() {
        return candidate;
    }
}
