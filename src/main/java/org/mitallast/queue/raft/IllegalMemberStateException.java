package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.StreamableError;

public class IllegalMemberStateException extends StreamableError {

    public IllegalMemberStateException() {
        this("illegal member state");
    }

    public IllegalMemberStateException(String message) {
        super(message);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StreamableError.Builder<Builder, IllegalMemberStateException> {

        @Override
        public IllegalMemberStateException build() {
            return new IllegalMemberStateException(message);
        }
    }
}