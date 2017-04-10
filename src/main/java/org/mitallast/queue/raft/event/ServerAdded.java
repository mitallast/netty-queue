package org.mitallast.queue.raft.event;

import org.mitallast.queue.transport.DiscoveryNode;

public class ServerAdded {
    private final DiscoveryNode node;

    public ServerAdded(DiscoveryNode node) {
        this.node = node;
    }

    public DiscoveryNode node() {
        return node;
    }
}
