package org.mitallast.queue.raft2.domain;

import org.mitallast.queue.raft2.Term;
import org.mitallast.queue.raft2.cluster.ClusterConfiguration;

import java.util.Optional;

public class WithNewConfigEvent implements DomainEvent {
    private final Optional<Term> term;
    private final ClusterConfiguration config;

    public WithNewConfigEvent(ClusterConfiguration config) {
        this(Optional.empty(), config);
    }

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
