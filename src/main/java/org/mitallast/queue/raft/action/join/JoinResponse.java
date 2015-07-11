package org.mitallast.queue.raft.action.join;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamableError;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class JoinResponse implements ActionResponse<JoinResponse> {
    private final StreamableError error;
    private final DiscoveryNode leader;
    private final long term;

    public JoinResponse(StreamableError error, DiscoveryNode leader, long term) {
        this.error = error;
        this.leader = leader;
        this.term = term;
    }

    public StreamableError error() {
        return error;
    }

    public DiscoveryNode leader() {
        return leader;
    }

    public long term() {
        return term;
    }

    @Override
    public String toString() {
        return "JoinResponse{" +
            "error=" + error +
            ", leader=" + leader +
            ", term=" + term +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<JoinResponse> {
        private StreamableError error;
        private DiscoveryNode leader;
        private long term;

        private Builder from(JoinResponse entry) {
            error = entry.error;
            leader = entry.leader;
            term = entry.term;
            return this;
        }

        public Builder setError(StreamableError error) {
            this.error = error;
            return this;
        }

        public Builder setLeader(DiscoveryNode leader) {
            this.leader = leader;
            return this;
        }

        public Builder setTerm(long term) {
            this.term = term;
            return this;
        }

        public JoinResponse build() {
            return new JoinResponse(error, leader, term);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            if (stream.readBoolean()) {
                error = stream.readError();
                return;
            }
            leader = stream.readStreamable(DiscoveryNode::new);
            term = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            if (error != null) {
                stream.writeBoolean(true);
                stream.writeError(error);
                return;
            }
            stream.writeBoolean(false);
            stream.writeStreamable(leader);
            stream.writeLong(term);
        }
    }
}
