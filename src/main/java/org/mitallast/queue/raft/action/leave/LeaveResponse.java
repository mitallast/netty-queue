package org.mitallast.queue.raft.action.leave;

import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftError;
import org.mitallast.queue.raft.action.RaftResponse;
import org.mitallast.queue.raft.action.ResponseStatus;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class LeaveResponse implements RaftResponse<LeaveResponse> {
    private final RaftError error;
    private final ResponseStatus status;
    private final DiscoveryNode member;

    public LeaveResponse(ResponseStatus status, RaftError error, DiscoveryNode member) {
        this.status = status;
        this.error = error;
        this.member = member;
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

    @Override
    public String toString() {
        return "LeaveResponse{" +
            "error=" + error +
            ", status=" + status +
            ", member=" + member +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<LeaveResponse> {
        private ResponseStatus status;
        private RaftError error;
        private DiscoveryNode member;

        private Builder from(LeaveResponse entry) {
            status = entry.status;
            error = entry.error;
            member = entry.member;
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

        @Override
        public LeaveResponse build() {
            return new LeaveResponse(status, error, member);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            status = stream.readEnum(ResponseStatus.class);
            if (!ResponseStatus.OK.equals(status)) {
                error = stream.readEnum(RaftError.class);
                return;
            }
            member = stream.readStreamable(DiscoveryNode::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeEnum(status);
            if (!ResponseStatus.OK.equals(status)) {
                stream.writeEnum(error);
                return;
            }
            stream.writeStreamable(member);
        }
    }
}
