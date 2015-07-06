package org.mitallast.queue.raft.resource;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Command;

import java.io.IOException;

public class ResourceCommand<T extends Command<U>, U extends Streamable>
    extends ResourceOperation<T, U>
    implements Command<U>, Entry<ResourceCommand<T, U>> {

    public ResourceCommand(long resource, T operation) {
        super(resource, operation);
    }

    @Override
    public Builder<T, U> toBuilder() {
        return new Builder<T, U>().from(this);
    }

    public static <T extends Command<U>, U extends Streamable> Builder<T, U> builder() {
        return new Builder<>();
    }

    public static class Builder<T extends Command<U>, U extends Streamable> implements EntryBuilder<ResourceCommand<T, U>> {
        private long resource;
        private T operation;

        public Builder<T, U> from(ResourceCommand<T, U> entry) {
            resource = entry.resource;
            operation = entry.operation;
            return this;
        }

        public Builder<T, U> setResource(long resource) {
            this.resource = resource;
            return this;
        }

        public Builder<T, U> setCommand(T operation) {
            this.operation = operation;
            return this;
        }

        @Override
        public ResourceCommand<T, U> build() {
            return new ResourceCommand<>(resource, operation);
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
            Entry operation = (Entry) this.operation;
            EntryBuilder builder = operation.toBuilder();
            stream.writeClass(builder.getClass());
            stream.writeStreamable(builder);
        }
    }
}
