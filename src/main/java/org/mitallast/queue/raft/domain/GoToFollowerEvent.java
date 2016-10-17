package org.mitallast.queue.raft.domain;

import org.mitallast.queue.raft.Term;

import java.util.Optional;

public class GoToFollowerEvent implements DomainEvent {

    private final Optional<Term> term;

    public GoToFollowerEvent() {
        this(Optional.empty());
    }

    public GoToFollowerEvent(Optional<Term> term) {
        this.term = term;
    }

    public Optional<Term> getTerm() {
        return term;
    }
}