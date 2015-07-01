package org.mitallast.queue.common.concurrent;

import java.util.concurrent.CompletableFuture;

public class Futures {
    public static <T> CompletableFuture<T> future() {
        return new CompletableFuture<>();
    }

    public static <T> CompletableFuture<T> complete(T value) {
        return CompletableFuture.completedFuture(value);
    }

    public static <T> CompletableFuture<T> completeExceptionally(Throwable exception) {
        CompletableFuture<T> future = future();
        future.completeExceptionally(exception);
        return future;
    }
}
