package org.mitallast.queue.raft.action.register;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftError;
import org.mitallast.queue.raft.action.RaftResponse;
import org.mitallast.queue.raft.action.ResponseStatus;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Collection;

public class RegisterResponse implements RaftResponse<RegisterResponse> {
    private final RaftError error;
    private final ResponseStatus status;
    private final long term;
    private final long session;
    private final DiscoveryNode leader;
    private final ImmutableList<DiscoveryNode> members;

    public RegisterResponse(ResponseStatus status, RaftError error, long term, long session, DiscoveryNode leader, ImmutableList<DiscoveryNode> members) {
        this.status = status;
        this.error = error;
        this.term = term;
        this.session = session;
        this.leader = leader;
        this.members = members;
    }

    public ResponseStatus status() {
        return status;
    }

    public RaftError error() {
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
            ", status=" + status +
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
        private ResponseStatus status;
        private RaftError error;
        private long term;
        private long session;
        private DiscoveryNode leader;
        private ImmutableList<DiscoveryNode> members;

        private Builder from(RegisterResponse entry) {
            status = entry.status;
            error = entry.error;
            term = entry.term;
            session = entry.session;
            leader = entry.leader;
            members = entry.members;
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
            return new RegisterResponse(status, error, term, session, leader, members);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            status = stream.readEnum(ResponseStatus.class);
            if (!ResponseStatus.OK.equals(status)) {
                error = stream.readEnum(RaftError.class);
                return;
            }
            term = stream.readLong();
            session = stream.readLong();
            leader = stream.readStreamable(DiscoveryNode::new);
            members = stream.readStreamableList(DiscoveryNode::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeEnum(status);
            if (!ResponseStatus.OK.equals(status)) {
                stream.writeEnum(error);
                return;
            }
            stream.writeLong(term);
            stream.writeLong(session);
            stream.writeStreamable(leader);
            stream.writeStreamableList(members);
        }
    }
}
