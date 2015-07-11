package org.mitallast.queue.raft.action.keepalive;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamableError;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Collection;

public class KeepAliveResponse implements ActionResponse<KeepAliveResponse> {
    private final StreamableError error;
    private final long version;
    private final long term;
    private final DiscoveryNode leader;
    private final ImmutableList<DiscoveryNode> members;

    public KeepAliveResponse(StreamableError error, long version, long term, DiscoveryNode leader, ImmutableList<DiscoveryNode> members) {
        this.error = error;
        this.version = version;
        this.term = term;
        this.leader = leader;
        this.members = members;
    }

    @Override
    public StreamableError error() {
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
        private StreamableError error;
        private long version;
        private long term;
        private DiscoveryNode leader;
        private ImmutableList<DiscoveryNode> members;

        private Builder from(KeepAliveResponse entry) {
            error = entry.error;
            version = entry.version;
            term = entry.term;
            leader = entry.leader;
            members = entry.members;
            return this;
        }

        public Builder setError(StreamableError error) {
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
            return new KeepAliveResponse(error, version, term, leader, members);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            if (stream.readBoolean()) {
                error = stream.readError();
                return;
            }
            version = stream.readLong();
            term = stream.readLong();
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
            stream.writeLong(version);
            stream.writeLong(term);
            stream.writeStreamable(leader);
            stream.writeStreamableList(members);
        }
    }
}
