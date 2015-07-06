package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.resource.result.BooleanResult;

import java.io.IOException;

public class DeleteResource implements Entry<DeleteResource>, Command<BooleanResult> {

    private final long resource;

    public DeleteResource(long resource) {
        this.resource = resource;
    }

    public long resource() {
        return resource;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<DeleteResource> {
        private long resource;

        public Builder from(DeleteResource entry) {
            resource = entry.resource;
            return this;
        }

        public Builder setResource(long resource) {
            this.resource = resource;
            return this;
        }

        @Override
        public DeleteResource build() {
            return new DeleteResource(resource);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            resource = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(resource);
        }
    }
}
