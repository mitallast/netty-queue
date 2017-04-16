package org.mitallast.queue.raft.event;

import javaslang.collection.Set;
import org.mitallast.queue.transport.DiscoveryNode;

public class MembersChanged {
    private final Set<DiscoveryNode> members;

    public MembersChanged(Set<DiscoveryNode> members) {
        this.members = members;
    }

    public Set<DiscoveryNode> members() {
        return members;
    }
}
