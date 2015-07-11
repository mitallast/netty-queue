package org.mitallast.queue.raft.action.vote;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamableError;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class VoteResponse implements ActionResponse<VoteResponse> {
    private final StreamableError error;
    private final DiscoveryNode member;
    private final long term;
    private final boolean voted;

    public VoteResponse(StreamableError error, DiscoveryNode member, long term, boolean voted) {
        this.error = error;
        this.member = member;
        this.term = term;
        this.voted = voted;
    }

    public StreamableError error() {
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
        private StreamableError error;
        private DiscoveryNode member;
        private long term;
        private boolean voted;

        private Builder from(VoteResponse entry) {
            error = entry.error;
            member = entry.member;
            term = entry.term;
            voted = entry.voted;
            return this;
        }

        public Builder setError(StreamableError error) {
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
            return new VoteResponse(error, member, term, voted);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            if (stream.readBoolean()) {
                error = stream.readError();
                return;
            }
            member = stream.readStreamableOrNull(DiscoveryNode::new);
            term = stream.readLong();
            voted = stream.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            if (error != null) {
                stream.writeBoolean(true);
                stream.writeError(error);
                return;
            }
            stream.writeBoolean(false);
            stream.writeStreamableOrNull(member);
            stream.writeLong(term);
            stream.writeBoolean(voted);
        }
    }
}
