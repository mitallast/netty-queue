package org.mitallast.queue.raft.action.poll;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamableError;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class PollResponse implements ActionResponse<PollResponse> {
    private final StreamableError error;
    private final DiscoveryNode member;
    private final long term;
    private final boolean accepted;

    public PollResponse(StreamableError error, DiscoveryNode member, long term, boolean accepted) {
        this.error = error;
        this.member = member;
        this.term = term;
        this.accepted = accepted;
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

    public boolean accepted() {
        return accepted;
    }

    @Override
    public String toString() {
        return "PollResponse{" +
            "error=" + error +
            ", member=" + member +
            ", term=" + term +
            ", accepted=" + accepted +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<PollResponse> {
        private StreamableError error;
        private DiscoveryNode member;
        private long term;
        private boolean accepted;

        private Builder from(PollResponse entry) {
            error = entry.error;
            member = entry.member;
            term = entry.term;
            accepted = entry.accepted;
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

        public Builder setAccepted(boolean accepted) {
            this.accepted = accepted;
            return this;
        }

        public PollResponse build() {
            return new PollResponse(error, member, term, accepted);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            if (stream.readBoolean()) {
                error = stream.readError();
                return;
            }
            member = stream.readStreamableOrNull(DiscoveryNode::new);
            term = stream.readLong();
            accepted = stream.readBoolean();
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
            stream.writeBoolean(accepted);
        }
    }
}
