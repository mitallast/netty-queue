package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.raft.action.RaftResponse;
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

    public abstract <T extends ActionRequest, R extends RaftResponse> CompletableFuture<R> send(T message);

    public abstract <T, U> CompletableFuture<U> send(Class<? super T> type, T message);

    public abstract <T, U> CompletableFuture<U> send(String topic, T message);

    public enum Type {
        PASSIVE, ACTIVE
    }
}
