package org.mitallast.queue.raft.action.register;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamableError;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Collection;

public class RegisterResponse implements ActionResponse<RegisterResponse> {
    private final StreamableError error;
    private final long term;
    private final long session;
    private final DiscoveryNode leader;
    private final ImmutableList<DiscoveryNode> members;

    public RegisterResponse(StreamableError error, long term, long session, DiscoveryNode leader, ImmutableList<DiscoveryNode> members) {
        this.error = error;
        this.term = term;
        this.session = session;
        this.leader = leader;
        this.members = members;
    }

    @Override
    public StreamableError error() {
        return error;
    }

    public long term() {
        return term;
    }

    public long session() {
        return session;
    }

    public DiscoveryNode leader() {
        return leader;
    }

    public ImmutableList<DiscoveryNode> members() {
        return members;
    }

    @Override
    public String toString() {
        return "RegisterResponse{" +
            "error=" + error +
            ", term=" + term +
            ", session=" + session +
            ", leader=" + leader +
            ", members=" + members +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<RegisterResponse> {
        private StreamableError error;
        private long term;
        private long session;
        private DiscoveryNode leader;
        private ImmutableList<DiscoveryNode> members;

        private Builder from(RegisterResponse entry) {
            error = entry.error;
            term = entry.term;
            session = entry.session;
            leader = entry.leader;
            members = entry.members;
            return this;
        }

        public Builder setError(StreamableError error) {
            this.error = error;
            return this;
        }

        public Builder setTerm(long term) {
            this.term = term;
            return this;
        }

        public Builder setSession(long session) {
            this.session = session;
            return this;
        }

        public Builder setLeader(DiscoveryNode leader) {
            this.leader = leader;
            return this;
        }

        public Builder setMembers(Collection<DiscoveryNode> members) {
            this.members = ImmutableList.copyOf(members);
            return this;
        }

        public RegisterResponse build() {
            return new RegisterResponse(error, term, session, leader, members);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            if (stream.readBoolean()) {
                error = stream.readError();
                return;
            }
            term = stream.readLong();
            session = stream.readLong();
            leader = stream.readStreamable(DiscoveryNode::new);
            members = stream.readStreamableList(DiscoveryNode::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            if (error != null) {
                stream.writeBoolean(true);
                stream.writeError(error);
                return;
            }
            stream.writeBoolean(false);
            stream.writeLong(term);
            stream.writeLong(session);
            stream.writeStreamable(leader);
            stream.writeStreamableList(members);
        }
    }
}
