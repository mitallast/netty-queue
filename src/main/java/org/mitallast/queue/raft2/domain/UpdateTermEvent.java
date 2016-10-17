package org.mitallast.queue.raft2.domain;

import org.mitallast.queue.raft2.Term;

public class UpdateTermEvent implements DomainEvent {
    private final Term term;

    public UpdateTermEvent(Term term) {
        this.term = term;
    }

    public Term getTerm() {
        return term;
    }
}
