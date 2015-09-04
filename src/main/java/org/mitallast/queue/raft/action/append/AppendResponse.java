package org.mitallast.queue.raft.action.append;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamableError;

import java.io.IOException;

public class AppendResponse implements ActionResponse<AppendResponse> {
    private final StreamableError error;
    private final long term;
    private final long logIndex;
    private final boolean succeeded;

    private AppendResponse(StreamableError error, long term, long logIndex, boolean succeeded) {
        this.error = error;
        this.term = term;
        this.logIndex = logIndex;
        this.succeeded = succeeded;
    }

    @Override
    public StreamableError error() {
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
        if (error == null) {
            return "AppendResponse{" +
                "term=" + term +
                ", logIndex=" + logIndex +
                ", succeeded=" + succeeded +
                '}';
        } else {
            return "AppendResponse{" +
                "error=" + error +
                '}';
        }
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<AppendResponse> {
        private StreamableError error;
        private long term;
        private long logIndex;
        private boolean succeeded;

        private Builder from(AppendResponse entry) {
            error = entry.error;
            logIndex = entry.logIndex;
            succeeded = entry.succeeded;
            return this;
        }

        public Builder setError(StreamableError error) {
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
            return new AppendResponse(error, term, logIndex, succeeded);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            if (stream.readBoolean()) {
                error = stream.readError();
                return;
            }
            term = stream.readLong();
            logIndex = stream.readLong();
            succeeded = stream.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            if (error != null) {
                stream.writeBoolean(true);
                stream.writeError(error);
                return;
            }
            stream.writeBoolean(false);
            stream.writeLong(term);
            stream.writeLong(logIndex);
            stream.writeBoolean(succeeded);
        }
    }
}
