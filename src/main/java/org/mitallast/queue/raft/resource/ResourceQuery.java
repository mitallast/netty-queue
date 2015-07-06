package org.mitallast.queue.raft.resource;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.ConsistencyLevel;
import org.mitallast.queue.raft.Query;

import java.io.IOException;

public class ResourceQuery<T extends Query<U>, U extends Streamable>
    extends ResourceOperation<T, U>
    implements Query<U>, Entry<ResourceQuery<T, U>> {

    public ResourceQuery(long resource, T operation) {
        super(resource, operation);
    }

    @Override
    public ConsistencyLevel consistency() {
        return operation.consistency();
    }

    @Override
    public Builder<T, U> toBuilder() {
        return new Builder<T, U>().from(this);
    }

    public static <T extends Query<U>, U extends Streamable> Builder<T, U> builder() {
        return new Builder<>();
    }

    public static class Builder<T extends Query<U>, U extends Streamable> implements EntryBuilder<ResourceQuery<T, U>> {
        private long resource;
        private T operation;

        public Builder<T, U> from(ResourceQuery<T, U> entry) {
            resource = entry.resource;
            operation = entry.operation;
            return this;
        }

        public Builder<T, U> setResource(long resource) {
            this.resource = resource;
            return this;
        }

        public Builder<T, U> setQuery(T query) {
            this.operation = query;
            return this;
        }

        @Override
        public ResourceQuery<T, U> build() {
            return new ResourceQuery<>(resource, operation);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            resource = stream.readLong();
            EntryBuilder<T> operationBuilder = stream.readStreamable();
            operation = operationBuilder.build();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(resource);
            Entry entry = (Entry) operation;
            EntryBuilder builder = entry.toBuilder();
            stream.writeClass(builder.getClass());
            stream.writeStreamable(builder);
        }
    }
}
