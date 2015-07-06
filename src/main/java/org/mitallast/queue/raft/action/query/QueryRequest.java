package org.mitallast.queue.raft.action.query;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;
import org.mitallast.queue.raft.Query;

import java.io.IOException;

public class QueryRequest implements ActionRequest<QueryRequest> {
    private final long session;
    private final long version;
    private final Query query;

    public QueryRequest(long session, long version, Query query) {
        this.session = session;
        this.version = version;
        this.query = query;
    }

    public long session() {
        return session;
    }

    public long version() {
        return version;
    }

    public Query query() {
        return query;
    }

    @Override
    public String toString() {
        return "QueryRequest{" +
            "session=" + session +
            ", version=" + version +
            ", query=" + query +
            '}';
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder();
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<QueryRequest> {
        private long session;
        private long version;
        private Query query;

        private Builder from(QueryRequest entry) {
            session = entry.session;
            version = entry.version;
            query = entry.query;
            return this;
        }

        public Builder setSession(long session) {
            this.session = session;
            return this;
        }

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public Builder setQuery(Query query) {
            this.query = query;
            return this;
        }

        public QueryRequest build() {
            return new QueryRequest(session, version, query);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            session = stream.readLong();
            version = stream.readLong();
            EntryBuilder<Query> queryBuilder = stream.readStreamable();
            query = queryBuilder.build();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(session);
            stream.writeLong(version);
            Entry entry = (Entry) query;
            EntryBuilder builder = entry.toBuilder();
            stream.writeClass(builder.getClass());
            stream.writeStreamable(builder);
        }
    }
}
