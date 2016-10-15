package org.mitallast.queue.raft2;

import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Optional;

public class LogEntry {
    private final Object command;
    private final Term term;
    private final long index;
    private final Optional<DiscoveryNode> client;

    public LogEntry(Object command, Term term, long index, Optional<DiscoveryNode> client) {
        this.command = command;
        this.term = term;
        this.index = index;
        this.client = client;
    }

    public Object getCommand() {
        return command;
    }

    public Term getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    public Optional<DiscoveryNode> getClient() {
        return client;
    }
}
