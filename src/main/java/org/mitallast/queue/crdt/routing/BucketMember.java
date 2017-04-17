package org.mitallast.queue.crdt.routing;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class BucketMember implements Streamable {

    enum State {
        OPENED, CLOSED;
    }

    private final DiscoveryNode member;

    private final State state;

    public BucketMember(DiscoveryNode member) {
        this(member, State.OPENED);
    }

    public BucketMember(DiscoveryNode member, State state) {
        this.member = member;
        this.state = state;
    }

    public BucketMember(StreamInput stream) {
        this.member = stream.readStreamable(DiscoveryNode::new);
        this.state = stream.readEnum(State.class);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeStreamable(member);
        stream.writeEnum(state);
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

    public BucketMember close() {
        return new BucketMember(member, State.CLOSED);
    }

    @Override
    public String toString() {
        return "{" + member + ", state=" + state + '}';
    }
}
