package org.mitallast.queue.raft.action.leave;

import com.google.common.base.Preconditions;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class LeaveRequest implements ActionRequest<LeaveRequest> {
    private final DiscoveryNode member;

    public LeaveRequest(DiscoveryNode member) {
        this.member = member;
    }

    public DiscoveryNode member() {
        return member;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder();
    }

    @Override
    public String toString() {
        return "LeaveRequest{" +
            "member=" + member +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<LeaveRequest> {
        private DiscoveryNode member;

        private Builder from(LeaveRequest entry) {
            member = entry.member;
            return this;
        }

        public Builder setMember(DiscoveryNode member) {
            this.member = member;
            return this;
        }

        public LeaveRequest build() {
            Preconditions.checkNotNull(member);
            return new LeaveRequest(member);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            member = stream.readStreamable(DiscoveryNode::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeStreamable(member);
        }
    }
}
