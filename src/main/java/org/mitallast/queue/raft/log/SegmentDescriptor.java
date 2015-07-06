package org.mitallast.queue.raft.log;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class SegmentDescriptor {
    private final long id;
    private final long index;
    private final long version;
    private final long maxEntrySize;
    private final long maxSegmentSize;
    private final long maxEntries;

    public SegmentDescriptor(long id, long index, long version, long maxEntrySize, long maxSegmentSize, long maxEntries) {
        this.id = id;
        this.index = index;
        this.version = version;
        this.maxEntrySize = maxEntrySize;
        this.maxSegmentSize = maxSegmentSize;
        this.maxEntries = maxEntries;
    }

    public long id() {
        return id;
    }

    public long version() {
        return version;
    }

    public long index() {
        return index;
    }

    public long maxEntrySize() {
        return maxEntrySize;
    }

    public long maxSegmentSize() {
        return maxSegmentSize;
    }

    public long maxEntries() {
        return maxEntries;
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements Streamable {
        private long id;
        private long index;
        private long version;
        private long maxEntrySize;
        private long maxSegmentSize;
        private long maxEntries;

        public Builder() {
        }

        public Builder(SegmentDescriptor descriptor) {
            id = descriptor.id;
            index = descriptor.index;
            version = descriptor.version;
            maxEntrySize = descriptor.maxEntrySize;
            maxSegmentSize = descriptor.maxSegmentSize;
            maxEntries = descriptor.maxEntries;
        }

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        public Builder setIndex(long index) {
            this.index = index;
            return this;
        }

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public Builder nextVersion() {
            return null;
        }

        public Builder setMaxEntrySize(long maxEntrySize) {
            this.maxEntrySize = maxEntrySize;
            return this;
        }

        public Builder setMaxSegmentSize(long maxSegmentSize) {
            this.maxSegmentSize = maxSegmentSize;
            return this;
        }

        public Builder setMaxEntries(long maxEntries) {
            this.maxEntries = maxEntries;
            return this;
        }

        public SegmentDescriptor build() {
            return new SegmentDescriptor(id, index, version, maxEntrySize, maxSegmentSize, maxEntries);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            id = stream.readLong();
            index = stream.readLong();
            version = stream.readLong();
            maxEntrySize = stream.readLong();
            maxSegmentSize = stream.readLong();
            maxEntries = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(id);
            stream.writeLong(index);
            stream.writeLong(version);
            stream.writeLong(maxEntrySize);
            stream.writeLong(maxSegmentSize);
            stream.writeLong(maxEntries);
        }
    }
}
