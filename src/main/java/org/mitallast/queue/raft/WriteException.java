package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.StreamableError;

public class WriteException extends StreamableError {

    public WriteException() {
        this("failed to obtain write quorum");
    }

    public WriteException(String message) {
        super(message);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StreamableError.Builder<Builder, WriteException> {

        @Override
        public WriteException build() {
            return new WriteException(message);
        }
    }
}