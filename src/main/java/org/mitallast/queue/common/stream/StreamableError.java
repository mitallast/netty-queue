package org.mitallast.queue.common.stream;

import org.mitallast.queue.common.builder.EntryBuilder;

import java.io.IOException;

public abstract class StreamableError extends RuntimeException {

    public StreamableError(String message) {
        super(message);
    }

    public abstract Builder toBuilder();

    public static abstract class Builder<B extends Builder<B, E>, E extends StreamableError> implements EntryBuilder<E> {
        protected String message;

        @SuppressWarnings("unchecked")
        public B from(E error) {
            message = error.getMessage();
            return (B) this;
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            message = stream.readText();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeText(message);
        }
    }
}
