package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.StreamableError;

public class InternalException extends StreamableError {

    public InternalException() {
        this("internal error");
    }

    public InternalException(String message) {
        super(message);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StreamableError.Builder<Builder, InternalException> {

        @Override
        public InternalException build() {
            return new InternalException(message);
        }
    }
}