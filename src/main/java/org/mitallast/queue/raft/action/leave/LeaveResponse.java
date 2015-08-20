package org.mitallast.queue.raft.action.leave;

import com.google.common.base.Preconditions;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamableError;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class LeaveResponse implements ActionResponse<LeaveResponse> {
    private final StreamableError error;
    private final DiscoveryNode member;

    public LeaveResponse(StreamableError error, DiscoveryNode member) {
        this.error = error;
        this.member = member;
    }

    public StreamableError error() {
        return error;
    }

    public DiscoveryNode member() {
        return member;
    }

    @Override
    public String toString() {
        return "LeaveResponse{" +
            "error=" + error +
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
        private StreamableError error;
        private DiscoveryNode member;

        private Builder from(LeaveResponse entry) {
            error = entry.error;
            member = entry.member;
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

        @Override
        public LeaveResponse build() {
            if (error == null) {
                Preconditions.checkNotNull(member);
            }
            return new LeaveResponse(error, member);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            if (stream.readBoolean()) {
                error = stream.readError();
                return;
            }
            member = stream.readStreamable(DiscoveryNode::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            if (error != null) {
                stream.writeBoolean(true);
                stream.writeError(error);
                return;
            }
            stream.writeBoolean(false);
            stream.writeStreamable(member);
        }
    }
}
