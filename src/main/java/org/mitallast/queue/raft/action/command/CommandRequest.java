package org.mitallast.queue.raft.action.command;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;
import org.mitallast.queue.raft.Command;

import java.io.IOException;

public class CommandRequest implements ActionRequest<CommandRequest> {
    private final long session;
    private final long request;
    private final long response;
    private final Command command;

    private CommandRequest(long session, long request, long response, Command command) {
        this.session = session;
        this.request = request;
        this.response = response;
        this.command = command;
    }

    public long session() {
        return session;
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
    public ValidationBuilder validate() {
        return ValidationBuilder.builder();
    }

    @Override
    public String toString() {
        return "CommandRequest{" +
            "session=" + session +
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

    public static class Builder implements EntryBuilder<CommandRequest> {
        private long session;
        private long request;
        private long response;
        private Command command;

        private Builder from(CommandRequest entry) {
            session = entry.session;
            request = entry.request;
            response = entry.response;
            command = entry.command;
            return this;
        }

        public Builder setSession(long session) {
            this.session = session;
            return this;
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

        public CommandRequest build() {
            return new CommandRequest(session, request, response, command);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            session = stream.readLong();
            request = stream.readLong();
            response = stream.readLong();
            EntryBuilder<Command> commandBuilder = stream.readStreamable();
            command = commandBuilder.build();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(session);
            stream.writeLong(request);
            stream.writeLong(response);
            Entry command = (Entry) this.command;
            EntryBuilder builder = command.toBuilder();
            stream.writeClass(builder.getClass());
            stream.writeStreamable(builder);
        }
    }

}
