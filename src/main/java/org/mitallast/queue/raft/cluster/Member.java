package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.transport.DiscoveryNode;

public class Member {
    protected Type type;
    private DiscoveryNode node;

    protected Member(DiscoveryNode node, Type type) {
        this.node = node;
        this.type = type;
    }

    public DiscoveryNode node() {
        return node;
    }

    public Type type() {
        return type;
    }

    public enum Type {
        PASSIVE, ACTIVE
    }
}
