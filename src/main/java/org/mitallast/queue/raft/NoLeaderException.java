package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.StreamableError;

public class NoLeaderException extends StreamableError {

    public NoLeaderException() {
        this("not the leader");
    }

    public NoLeaderException(String message) {
        super(message);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StreamableError.Builder<Builder, NoLeaderException> {

        @Override
        public NoLeaderException build() {
            return new NoLeaderException(message);
        }
    }
}
