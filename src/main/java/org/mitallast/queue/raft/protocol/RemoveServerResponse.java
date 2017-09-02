package org.mitallast.queue.raft.protocol;

import javaslang.control.Option;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class RemoveServerResponse implements Message {
    public static final Codec<RemoveServerResponse> codec = Codec.Companion.of(
        RemoveServerResponse::new,
        RemoveServerResponse::getStatus,
        RemoveServerResponse::getLeader,
        Codec.Companion.enumCodec(Status.class),
        Codec.Companion.optionCodec(DiscoveryNode.codec)
    );

    public enum Status {OK, TIMEOUT, NOT_LEADER}

    private final Status status;
    private final Option<DiscoveryNode> leader;

    public RemoveServerResponse(Status status, Option<DiscoveryNode> leader) {
        this.status = status;
        this.leader = leader;
    }

    public Status getStatus() {
        return status;
    }

    public Option<DiscoveryNode> getLeader() {
        return leader;
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
