package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.StreamableError;

public class ReadException extends StreamableError {

    public ReadException() {
        this("failed to obtain read quorum");
    }

    public ReadException(String message) {
        super(message);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StreamableError.Builder<Builder, ReadException> {

        @Override
        public ReadException build() {
            return new ReadException(message);
        }
    }
}