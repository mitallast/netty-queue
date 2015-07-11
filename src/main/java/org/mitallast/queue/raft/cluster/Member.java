package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.concurrent.CompletableFuture;

public abstract class Member {
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

    public abstract <T extends ActionRequest, R extends ActionResponse> CompletableFuture<R> send(T message);

    public enum Type {
        PASSIVE, ACTIVE
    }
}
