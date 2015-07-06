package org.mitallast.queue.raft.action.append;

import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftError;
import org.mitallast.queue.raft.action.RaftResponse;
import org.mitallast.queue.raft.action.ResponseStatus;

import java.io.IOException;

public class AppendResponse implements RaftResponse<AppendResponse> {
    private final ResponseStatus status;
    private final RaftError error;
    private final long term;
    private final long logIndex;
    private final boolean succeeded;

    private AppendResponse(ResponseStatus status, RaftError error, long term, long logIndex, boolean succeeded) {
        this.status = status;
        this.error = error;
        this.term = term;
        this.logIndex = logIndex;
        this.succeeded = succeeded;
    }

    public ResponseStatus status() {
        return status;
    }

    public RaftError error() {
        return error;
    }

    public long term() {
        return term;
    }

    public long logIndex() {
        return logIndex;
    }

    public boolean succeeded() {
        return succeeded;
    }

    @Override
    public String toString() {
        return "AppendResponse{" +
            "status=" + status +
            ", error=" + error +
            ", term=" + term +
            ", logIndex=" + logIndex +
            ", succeeded=" + succeeded +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<AppendResponse> {
        private ResponseStatus status;
        private RaftError error;
        private long term;
        private long logIndex;
        private boolean succeeded;

        private Builder from(AppendResponse entry) {
            status = entry.status;
            error = entry.error;
            logIndex = entry.logIndex;
            succeeded = entry.succeeded;
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

        public Builder setTerm(long term) {
            this.term = term;
            return this;
        }

        public Builder setLogIndex(long logIndex) {
            this.logIndex = logIndex;
            return this;
        }

        public Builder setSucceeded(boolean succeeded) {
            this.succeeded = succeeded;
            return this;
        }

        public AppendResponse build() {
            return new AppendResponse(status, error, term, logIndex, succeeded);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            status = stream.readEnum(ResponseStatus.class);
            if (!ResponseStatus.OK.equals(status)) {
                error = stream.readEnum(RaftError.class);
                return;
            }
            term = stream.readLong();
            logIndex = stream.readLong();
            succeeded = stream.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeEnum(status);
            if (!ResponseStatus.OK.equals(status)) {
                stream.writeEnum(error);
                return;
            }
            stream.writeLong(term);
            stream.writeLong(logIndex);
            stream.writeBoolean(succeeded);
        }
    }
}
