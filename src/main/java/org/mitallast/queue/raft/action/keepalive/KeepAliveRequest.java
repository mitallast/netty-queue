package org.mitallast.queue.raft.action.keepalive;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class KeepAliveRequest implements ActionRequest<KeepAliveRequest> {
    private final long session;

    public KeepAliveRequest(long session) {
        this.session = session;
    }

    public long session() {
        return session;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder();
    }

    @Override
    public String toString() {
        return "KeepAliveRequest{" +
            "session=" + session +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<KeepAliveRequest> {
        private long session;

        private Builder from(KeepAliveRequest entry) {
            session = entry.session;
            return this;
        }

        public Builder setSession(long session) {
            this.session = session;
            return this;
        }

        @Override
        public KeepAliveRequest build() {
            return new KeepAliveRequest(session);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            session = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(session);
        }
    }
}
