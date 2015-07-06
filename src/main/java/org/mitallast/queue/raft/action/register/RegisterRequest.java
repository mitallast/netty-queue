package org.mitallast.queue.raft.action.register;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RegisterRequest implements ActionRequest<RegisterRequest> {
    private final DiscoveryNode member;

    public RegisterRequest(DiscoveryNode member) {
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
        return "RegisterRequest{" +
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

    public static class Builder implements EntryBuilder<RegisterRequest> {
        private DiscoveryNode member;

        private Builder from(RegisterRequest entry) {
            member = entry.member;
            return this;
        }

        public Builder setMember(DiscoveryNode member) {
            this.member = member;
            return this;
        }

        public RegisterRequest build() {
            return new RegisterRequest(member);
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
