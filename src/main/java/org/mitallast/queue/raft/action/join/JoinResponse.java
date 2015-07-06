package org.mitallast.queue.raft.action.join;

import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftError;
import org.mitallast.queue.raft.action.RaftResponse;
import org.mitallast.queue.raft.action.ResponseStatus;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class JoinResponse implements RaftResponse<JoinResponse> {
    private final RaftError error;
    private final ResponseStatus status;
    private final DiscoveryNode leader;
    private final long term;

    public JoinResponse(ResponseStatus status, RaftError error, DiscoveryNode leader, long term) {
        this.status = status;
        this.error = error;
        this.leader = leader;
        this.term = term;
    }

    public ResponseStatus status() {
        return status;
    }

    public RaftError error() {
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
            ", status=" + status +
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
        private ResponseStatus status;
        private RaftError error;
        private DiscoveryNode leader;
        private long term;

        private Builder from(JoinResponse entry) {
            status = entry.status;
            error = entry.error;
            leader = entry.leader;
            term = entry.term;
            return this;
        }

        public Builder setStatus(ResponseStatus status) {
            this.status = status;
            return this;
        }

        public Builder setError(RaftError error) {
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
            return new JoinResponse(status, error, leader, term);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            status = stream.readEnum(ResponseStatus.class);
            if (!ResponseStatus.OK.equals(status)) {
                error = stream.readEnum(RaftError.class);
                return;
            }
            leader = stream.readStreamable(DiscoveryNode::new);
            term = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeEnum(status);
            if (!ResponseStatus.OK.equals(status)) {
                stream.writeEnum(error);
                return;
            }
            stream.writeStreamable(leader);
            stream.writeLong(term);
        }
    }
}
