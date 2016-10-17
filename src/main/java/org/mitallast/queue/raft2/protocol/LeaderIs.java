package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Optional;

public class LeaderIs implements RaftMessage {
    private Optional<DiscoveryNode> ref;
    private Optional<Streamable> msg;

    protected LeaderIs() {
    }

    public LeaderIs(Optional<DiscoveryNode> ref, Optional<Streamable> msg) {
        this.ref = ref;
        this.msg = msg;
    }

    public Optional<DiscoveryNode> getRef() {
        return ref;
    }

    public Optional<Streamable> getMsg() {
        return msg;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        ref = Optional.ofNullable(stream.readStreamableOrNull(DiscoveryNode::new));
        if (stream.readBoolean()) {
            msg = Optional.of(stream.readStreamable());
        } else {
            msg = Optional.empty();
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamableOrNull(ref.orElse(null));
        if (msg.isPresent()) {
            stream.writeBoolean(true);
            stream.writeClass(msg.get().getClass());
            stream.writeStreamable(msg.get());
        } else {
            stream.writeBoolean(false);
        }
    }
}
