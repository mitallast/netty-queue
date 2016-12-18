package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Optional;

public class RemoveServerResponse implements Streamable {
    public enum Status {OK, TIMEOUT, NOT_LEADER}

    private final Status status;
    private final Optional<DiscoveryNode> leader;

    public RemoveServerResponse(Status status, Optional<DiscoveryNode> leader) {
        this.status = status;
        this.leader = leader;
    }

    public RemoveServerResponse(StreamInput stream) throws IOException {
        status = stream.readEnum(Status.class);
        leader = Optional.ofNullable(stream.readStreamableOrNull(DiscoveryNode::new));
    }

    public Status getStatus() {
        return status;
    }

    public Optional<DiscoveryNode> getLeader() {
        return leader;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeEnum(status);
        stream.writeStreamableOrNull(leader.orElse(null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoveServerResponse that = (RemoveServerResponse) o;

        if (status != that.status) return false;
        return leader.equals(that.leader);
    }

    @Override
    public int hashCode() {
        int result = status.hashCode();
        result = 31 * result + leader.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RemoveServerResponse{" +
            "status=" + status +
            ", leader=" + leader +
            '}';
    }
}
