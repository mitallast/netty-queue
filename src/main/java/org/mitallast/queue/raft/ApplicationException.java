package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.StreamableError;

public class ApplicationException extends StreamableError {

    public ApplicationException() {
        this("an application error occurred");
    }

    public ApplicationException(String message) {
        super(message);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StreamableError.Builder<Builder, ApplicationException> {

        @Override
        public ApplicationException build() {
            return new ApplicationException(message);
        }
    }
}