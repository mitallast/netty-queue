package org.mitallast.queue.raft.action.vote;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class VoteRequest implements ActionRequest<VoteRequest> {
    private final DiscoveryNode candidate;
    private final long term;
    private final long logIndex;
    private final long logTerm;

    public VoteRequest(DiscoveryNode candidate, long term, long logIndex, long logTerm) {
        this.candidate = candidate;
        this.term = term;
        this.logIndex = logIndex;
        this.logTerm = logTerm;
    }

    public DiscoveryNode candidate() {
        return candidate;
    }

    public long term() {
        return term;
    }

    public long logIndex() {
        return logIndex;
    }

    public long logTerm() {
        return logTerm;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder();
    }

    @Override
    public String toString() {
        return "VoteRequest{" +
            "candidate=" + candidate +
            ", term=" + term +
            ", logIndex=" + logIndex +
            ", logTerm=" + logTerm +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<VoteRequest> {
        private DiscoveryNode candidate;
        private long term;
        private long logIndex;
        private long logTerm;

        private Builder from(VoteRequest entry) {
            candidate = entry.candidate;
            term = entry.term;
            logIndex = entry.logIndex;
            logTerm = entry.logTerm;
            return this;
        }

        public Builder setCandidate(DiscoveryNode candidate) {
            this.candidate = candidate;
            return this;
        }

        public Builder setTerm(long term) {
            this.term = term;
            return this;
        }

        public Builder setLogIndex(long logIndex) {
            this.logIndex = logIndex;
            return this;
        }

        public Builder setLogTerm(long logTerm) {
            this.logTerm = logTerm;
            return this;
        }

        public VoteRequest build() {
            return new VoteRequest(candidate, term, logIndex, logTerm);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            candidate = stream.readStreamable(DiscoveryNode::new);
            term = stream.readLong();
            logIndex = stream.readLong();
            logTerm = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeStreamable(candidate);
            stream.writeLong(term);
            stream.writeLong(logIndex);
            stream.writeLong(logTerm);
        }
    }
}
