package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.Query;

import java.io.IOException;

public class QueryEntry extends OperationEntry<QueryEntry> {
    private final long version;
    private final Query query;

    public QueryEntry(long index, long term, long timestamp, long session, long version, Query query) {
        super(index, term, timestamp, session);
        this.version = version;
        this.query = query;
    }

    public long version() {
        return version;
    }

    public Query query() {
        return query;
    }

    @Override
    public String toString() {
        return "QueryEntry{" +
            "index=" + index +
            ", term=" + term +
            ", timestamp=" + timestamp +
            ", session=" + session +
            ", version=" + version +
            ", query=" + query +
            "}";
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends OperationEntry.Builder<Builder, QueryEntry> {
        private long version;
        private Query query;

        public Builder from(QueryEntry entry) {
            version = entry.version;
            query = entry.query;
            return super.from(entry);
        }

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public Builder setQuery(Query query) {
            this.query = query;
            return this;
        }

        public QueryEntry build() {
            return new QueryEntry(index, term, timestamp, session, version, query);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            super.readFrom(stream);
            version = stream.readLong();
            EntryBuilder<Query> queryBuilder = stream.readStreamable();
            query = queryBuilder.build();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            super.writeTo(stream);
            stream.writeLong(version);
            Entry entry = (Entry) this.query;
            EntryBuilder builder = entry.toBuilder();
            stream.writeClass(builder.getClass());
            stream.writeStreamable(builder);
        }
    }
}
