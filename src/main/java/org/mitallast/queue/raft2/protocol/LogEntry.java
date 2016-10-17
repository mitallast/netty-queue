package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft2.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Optional;

public class LogEntry implements Streamable {
    private Term term;
    private long index;
    private Streamable command;
    private transient Optional<DiscoveryNode> client;

    protected LogEntry() {
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
    public void readFrom(StreamInput stream) throws IOException {
        term = new Term(stream.readLong());
        index = stream.readLong();
        command = stream.readStreamable();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(term.getTerm());
        stream.writeLong(index);
        stream.writeClass(command.getClass());
        stream.writeStreamable(command);
        client = Optional.empty();
    }
}
