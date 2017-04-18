package org.mitallast.queue.crdt.routing;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class RoutingReplica implements Streamable {

    public enum State {
        OPENED, CLOSED;
    }

    private final long id;
    private final DiscoveryNode member;
    private final State state;

    public RoutingReplica(long id, DiscoveryNode member) {
        this(id, member, State.OPENED);
    }

    public RoutingReplica(long id, DiscoveryNode member, State state) {
        this.id = id;
        this.member = member;
        this.state = state;
    }

    public RoutingReplica(StreamInput stream) {
        this.id = stream.readInt();
        this.member = stream.readStreamable(DiscoveryNode::new);
        this.state = stream.readEnum(State.class);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeLong(id);
        stream.writeStreamable(member);
        stream.writeEnum(state);
    }

    public long id() {
        return id;
    }

    public DiscoveryNode member() {
        return member;
    }

    public State state() {
        return state;
    }

    public boolean isOpened() {
        return state == State.OPENED;
    }

    public boolean isClosed() {
        return state == State.CLOSED;
    }

    public RoutingReplica close() {
        return new RoutingReplica(id, member, State.CLOSED);
    }

    @Override
    public String toString() {
        return "RoutingReplica{id=" + id + ", " + member + ", state=" + state + '}';
    }
}
