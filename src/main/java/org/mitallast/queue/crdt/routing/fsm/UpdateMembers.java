package org.mitallast.queue.crdt.routing.fsm;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class UpdateMembers implements Streamable {
    private final ImmutableSet<DiscoveryNode> members;

    public UpdateMembers(ImmutableSet<DiscoveryNode> members) {
        this.members = members;
    }

    public UpdateMembers(StreamInput stream) throws IOException {
        members = stream.readStreamableSet(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamableSet(members);
    }

    public ImmutableSet<DiscoveryNode> members() {
        return members;
    }
}
