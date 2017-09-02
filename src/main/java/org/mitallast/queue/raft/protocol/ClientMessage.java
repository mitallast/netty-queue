package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class ClientMessage implements Message {
    public static final Codec<ClientMessage> codec = Codec.Companion.of(
        ClientMessage::new,
        ClientMessage::command,
        ClientMessage::session,
        Codec.Companion.anyCodec(),
        Codec.Companion.longCodec()
    );

    private final Message command;
    private final long session;

    public ClientMessage(Message command, long session) {
        this.command = command;
        this.session = session;
    }

    public Message command() {
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

        if (session != that.session) return false;
        return command != null ? command.equals(that.command) : that.command == null;
    }

    @Override
    public int hashCode() {
        int result = command != null ? command.hashCode() : 0;
        result = 31 * result + (int) (session ^ (session >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ClientMessage{command=" + command + '}';
    }
}
