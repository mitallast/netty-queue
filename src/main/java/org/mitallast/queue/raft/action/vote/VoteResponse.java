package org.mitallast.queue.raft.action.vote;

import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftError;
import org.mitallast.queue.raft.action.RaftResponse;
import org.mitallast.queue.raft.action.ResponseStatus;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class VoteResponse implements RaftResponse<VoteResponse> {
    private final RaftError error;
    private final ResponseStatus status;
    private final DiscoveryNode member;
    private final long term;
    private final boolean voted;

    public VoteResponse(ResponseStatus status, RaftError error, DiscoveryNode member, long term, boolean voted) {
        this.status = status;
        this.error = error;
        this.member = member;
        this.term = term;
        this.voted = voted;
    }

    public ResponseStatus status() {
        return status;
    }

    public RaftError error() {
        return error;
    }

    public DiscoveryNode member() {
        return member;
    }

    public long term() {
        return term;
    }

    public boolean voted() {
        return voted;
    }

    @Override
    public String toString() {
        return "VoteResponse{" +
            "error=" + error +
            ", status=" + status +
            ", member=" + member +
            ", term=" + term +
            ", voted=" + voted +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<VoteResponse> {
        private ResponseStatus status = ResponseStatus.OK;
        private RaftError error;
        private DiscoveryNode member;
        private long term;
        private boolean voted;

        private Builder from(VoteResponse entry) {
            status = entry.status;
            error = entry.error;
            member = entry.member;
            term = entry.term;
            voted = entry.voted;
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

        public Builder setMember(DiscoveryNode member) {
            this.member = member;
            return this;
        }

        public Builder setTerm(long term) {
            this.term = term;
            return this;
        }

        public Builder setVoted(boolean voted) {
            this.voted = voted;
            return this;
        }

        public VoteResponse build() {
            return new VoteResponse(status, error, member, term, voted);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            status = stream.readEnum(ResponseStatus.class);
            if (!ResponseStatus.OK.equals(status)) {
                error = stream.readEnum(RaftError.class);
                return;
            }
            member = stream.readStreamableOrNull(DiscoveryNode::new);
            term = stream.readLong();
            voted = stream.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeEnum(status);
            if (!ResponseStatus.OK.equals(status)) {
                stream.writeEnum(error);
                return;
            }
            stream.writeStreamableOrNull(member);
            stream.writeLong(term);
            stream.writeBoolean(voted);
        }
    }
}
