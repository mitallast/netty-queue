package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.StreamableError;

public class UnknownSessionException extends StreamableError {

    public UnknownSessionException() {
        this("unknown member session");
    }

    public UnknownSessionException(String message) {
        super(message);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StreamableError.Builder<Builder, UnknownSessionException> {

        @Override
        public UnknownSessionException build() {
            return new UnknownSessionException(message);
        }
    }
}