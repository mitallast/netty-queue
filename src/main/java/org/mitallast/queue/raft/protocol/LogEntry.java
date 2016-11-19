package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Optional;

public class LogEntry implements Streamable {
    private final Term term;
    private final long index;
    private final Streamable command;
    private final Optional<DiscoveryNode> client;

    public LogEntry(StreamInput stream) throws IOException {
        term = new Term(stream.readLong());
        index = stream.readLong();
        command = stream.readStreamable();
        client = Optional.ofNullable(stream.readStreamableOrNull(DiscoveryNode::new));
    }

    public LogEntry(Streamable command, Term term, long index) {
        this(command, term, index, Optional.empty());
    }

    public LogEntry(Streamable command, Term term, long index, Optional<DiscoveryNode> client) {
        this.command = command;
        this.term = term;
        this.index = index;
        this.client = client;
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

    public Streamable getCommand() {
        return command;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(term.getTerm());
        stream.writeLong(index);
        stream.writeClass(command.getClass());
        stream.writeStreamable(command);
        stream.writeStreamableOrNull(client.orElse(null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogEntry logEntry = (LogEntry) o;

        if (index != logEntry.index) return false;
        if (!term.equals(logEntry.term)) return false;
        if (!command.equals(logEntry.command)) return false;
        return client.equals(logEntry.client);

    }

    @Override
    public int hashCode() {
        int result = term.hashCode();
        result = 31 * result + (int) (index ^ (index >>> 32));
        result = 31 * result + command.hashCode();
        result = 31 * result + client.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry{" + term.getTerm() + ',' + index +
            ',' + command.getClass().getSimpleName() +
            ',' + client.map(DiscoveryNode::toString).orElse("") +
            '}';
    }
}
