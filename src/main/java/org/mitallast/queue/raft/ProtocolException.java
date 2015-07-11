package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.StreamableError;

public class ProtocolException extends StreamableError {

    public ProtocolException() {
        this("protocol error");
    }

    public ProtocolException(String message) {
        super(message);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StreamableError.Builder<Builder, ProtocolException> {

        @Override
        public ProtocolException build() {
            return new ProtocolException(message);
        }
    }
}