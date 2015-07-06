package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.Streamable;

import java.util.concurrent.CompletableFuture;

public interface Protocol {

    default <T extends Streamable> CompletableFuture<T> submit(Operation<T> operation) {
        if (operation instanceof Command) {
            return submit((Command<T>) operation);
        } else if (operation instanceof Query) {
            return submit((Query<T>) operation);
        } else {
            throw new IllegalArgumentException("unknown operation type");
        }
    }

    <T extends Streamable> CompletableFuture<T> submit(Command<T> command);

    <T extends Streamable> CompletableFuture<T> submit(Query<T> query);

    void delete();
}
