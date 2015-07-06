package org.mitallast.queue.raft.action.command;

import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.RaftError;
import org.mitallast.queue.raft.action.RaftResponse;
import org.mitallast.queue.raft.action.ResponseStatus;

import java.io.IOException;

public class CommandResponse implements RaftResponse<CommandResponse> {
    protected final RaftError error;
    protected final ResponseStatus status;
    private final long version;
    private final Streamable result;

    private CommandResponse(ResponseStatus status, RaftError error, long version, Streamable result) {
        this.status = status;
        this.error = error;
        this.version = version;
        this.result = result;
    }

    public ResponseStatus status() {
        return status;
    }

    public RaftError error() {
        return error;
    }

    public long version() {
        return version;
    }

    public Streamable result() {
        return result;
    }

    @Override
    public String toString() {
        return "CommandResponse{" +
            "error=" + error +
            ", status=" + status +
            ", version=" + version +
            ", result=" + result +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return null;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<CommandResponse> {
        private ResponseStatus status;
        private RaftError error;
        private long version;
        private Streamable result;

        public Builder from(CommandResponse entry) {
            status = entry.status;
            error = entry.error;
            version = entry.version;
            result = entry.result;
            return this;
        }

        public Builder setStatus(ResponseStatus status) {
            this.status = status;
            return this;
        }

        public Builder setError(RaftError error) {
            this.error = error;
            return this;
        }

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public Builder setResult(Streamable result) {
            this.result = result;
            return this;
        }

        public CommandResponse build() {
            return new CommandResponse(status, error, version, result);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            status = stream.readEnum(ResponseStatus.class);
            if (!ResponseStatus.OK.equals(status)) {
                error = stream.readEnum(RaftError.class);
                return;
            }
            version = stream.readLong();
            result = stream.readStreamable();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeEnum(status);
            if (!ResponseStatus.OK.equals(status)) {
                stream.writeEnum(error);
                return;
            }
            stream.writeLong(version);
            stream.writeClass(result.getClass());
            stream.writeStreamable(result);
        }
    }

}
