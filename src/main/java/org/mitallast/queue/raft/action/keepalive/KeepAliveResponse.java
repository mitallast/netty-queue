package org.mitallast.queue.raft.action.keepalive;

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

public class KeepAliveResponse implements RaftResponse<KeepAliveResponse> {
    private final RaftError error;
    private final ResponseStatus status;
    private final long version;
    private final long term;
    private final DiscoveryNode leader;
    private final ImmutableList<DiscoveryNode> members;

    public KeepAliveResponse(ResponseStatus status, RaftError error, long version, long term, DiscoveryNode leader, ImmutableList<DiscoveryNode> members) {
        this.status = status;
        this.error = error;
        this.version = version;
        this.term = term;
        this.leader = leader;
        this.members = members;
    }

    public ResponseStatus status() {
        return status;
    }

    public RaftError error() {
        return error;
    }

    public long version() {
        return version;
    }

    public long term() {
        return term;
    }

    public DiscoveryNode leader() {
        return leader;
    }

    public ImmutableList<DiscoveryNode> members() {
        return members;
    }

    @Override
    public String toString() {
        return "KeepAliveResponse{" +
            "error=" + error +
            ", status=" + status +
            ", version=" + version +
            ", term=" + term +
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

    public static class Builder implements EntryBuilder<KeepAliveResponse> {
        private ResponseStatus status;
        private RaftError error;
        private long version;
        private long term;
        private DiscoveryNode leader;
        private ImmutableList<DiscoveryNode> members;

        private Builder from(KeepAliveResponse entry) {
            status = entry.status;
            error = entry.error;
            version = entry.version;
            term = entry.term;
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

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public Builder setTerm(long term) {
            this.term = term;
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

        @Override
        public KeepAliveResponse build() {
            return new KeepAliveResponse(status, error, version, term, leader, members);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            status = stream.readEnum(ResponseStatus.class);
            if (!ResponseStatus.OK.equals(status)) {
                error = stream.readEnum(RaftError.class);
                return;
            }
            version = stream.readLong();
            term = stream.readLong();
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
            stream.writeLong(version);
            stream.writeLong(term);
            stream.writeStreamable(leader);
            stream.writeStreamableList(members);
        }
    }
}
