package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Operation;

import java.io.IOException;

public abstract class PathOperation<T extends Streamable, E extends PathOperation<T, E>> implements Entry<E>, Operation<T> {

    protected final String path;

    protected PathOperation(String path) {
        this.path = path;
    }

    public String path() {
        return path;
    }

    public abstract static class Builder<T extends Streamable, B extends Builder<T, B, E>, E extends PathOperation<T, E>> implements EntryBuilder<E> {
        protected String path;

        @SuppressWarnings("unchecked")
        public B from(E entry) {
            path = entry.path;
            return (B) this;
        }

        @SuppressWarnings("unchecked")
        public B setPath(String path) {
            this.path = path;
            return (B) this;
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            path = stream.readText();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeText(path);
        }
    }
}
