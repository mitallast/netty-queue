package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.Command;

import java.io.IOException;

public class CommandEntry extends OperationEntry<CommandEntry> {
    private final long request;
    private final long response;
    private final Command command;

    public CommandEntry(long index, long term, long timestamp, long session, long request, long response, Command command) {
        super(index, term, timestamp, session);
        this.request = request;
        this.response = response;
        this.command = command;
    }

    public long request() {
        return request;
    }

    public long response() {
        return response;
    }

    public Command command() {
        return command;
    }

    @Override
    public String toString() {
        return "CommandEntry{" +
            "index=" + index +
            ", term=" + term +
            ", timestamp=" + timestamp +
            ", session=" + session +
            ", request=" + request +
            ", response=" + response +
            ", command=" + command +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends OperationEntry.Builder<Builder, CommandEntry> {
        private long request;
        private long response;
        private Command command;

        public Builder from(CommandEntry entry) {
            request = entry.request;
            response = entry.response;
            command = entry.command;
            return super.from(entry);
        }

        public Builder setRequest(long request) {
            this.request = request;
            return this;
        }

        public Builder setResponse(long response) {
            this.response = response;
            return this;
        }

        public Builder setCommand(Command command) {
            this.command = command;
            return this;
        }

        public CommandEntry build() {
            return new CommandEntry(index, term, timestamp, session, request, response, command);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            super.readFrom(stream);
            request = stream.readLong();
            response = stream.readLong();
            EntryBuilder<Command> commandBuilder = stream.readStreamable();
            command = commandBuilder.build();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            super.writeTo(stream);
            stream.writeLong(request);
            stream.writeLong(response);
            Entry entry = (Entry) command;
            EntryBuilder builder = entry.toBuilder();
            stream.writeClass(builder.getClass());
            stream.writeStreamable(builder);
        }
    }
}
