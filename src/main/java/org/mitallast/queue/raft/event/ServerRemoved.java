package org.mitallast.queue.raft.event;

import org.mitallast.queue.transport.DiscoveryNode;

public class ServerRemoved {
    private final DiscoveryNode node;

    public ServerRemoved(DiscoveryNode node) {
        this.node = node;
    }

    public DiscoveryNode node() {
        return node;
    }
}
