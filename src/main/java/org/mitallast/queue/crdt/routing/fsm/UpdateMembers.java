package org.mitallast.queue.crdt.routing.fsm;

import javaslang.collection.Set;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;


public class UpdateMembers implements Streamable {
    private final Set<DiscoveryNode> members;

    public UpdateMembers(Set<DiscoveryNode> members) {
        this.members = members;
    }

    public UpdateMembers(StreamInput stream) {
        members = stream.readSet(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeSet(members);
    }

    public Set<DiscoveryNode> members() {
        return members;
    }
}
