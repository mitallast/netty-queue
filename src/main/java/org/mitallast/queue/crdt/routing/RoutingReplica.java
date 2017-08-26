package org.mitallast.queue.crdt.routing;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class RoutingReplica implements Message {
    public static final Codec<RoutingReplica> codec = Codec.of(
        RoutingReplica::new,
        RoutingReplica::id,
        RoutingReplica::member,
        RoutingReplica::state,
        Codec.longCodec,
        DiscoveryNode.codec,
        Codec.enumCodec(State.class)
    );

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
