package org.mitallast.queue.raft.cluster;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface MessageHandler<T, U> {

    CompletableFuture<U> handle(T message);
}
