package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

public class ClientMessage implements Streamable {
    private final Streamable command;
    private final long session;

    public ClientMessage(Streamable command, long session) {
        this.command = command;
        this.session = session;
    }

    public ClientMessage(StreamInput stream) {
        command = stream.readStreamable();
        session = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeClass(command.getClass());
        stream.writeStreamable(command);
        stream.writeLong(session);
    }

    public Streamable command() {
        return command;
    }

    public long session() {
        return session;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientMessage that = (ClientMessage) o;

        return command.equals(that.command);
    }

    @Override
    public int hashCode() {
        return command.hashCode();
    }

    @Override
    public String toString() {
        return "ClientMessage{command=" + command + '}';
    }
}
