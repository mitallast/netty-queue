package org.mitallast.queue.raft.event;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.transport.DiscoveryNode;

public class MembersChanged {
    private final ImmutableSet<DiscoveryNode> members;

    public MembersChanged(ImmutableSet<DiscoveryNode> members) {
        this.members = members;
    }

    public ImmutableSet<DiscoveryNode> members() {
        return members;
    }
}
