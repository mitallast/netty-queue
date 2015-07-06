package org.mitallast.queue.raft.resource;

import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.Operation;
import org.mitallast.queue.raft.Protocol;
import org.mitallast.queue.raft.Query;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractResource implements Resource {
    protected final Protocol protocol;

    protected AbstractResource(Protocol protocol) {
        this.protocol = protocol;
    }

    protected <T extends Streamable> CompletableFuture<T> submit(Operation<T> operation) {
        return protocol.submit(operation);
    }

    protected <T extends Streamable> CompletableFuture<T> submit(Command<T> command) {
        return protocol.submit(command);
    }

    protected <T extends Streamable> CompletableFuture<T> submit(Query<T> query) {
        return protocol.submit(query);
    }

    @Override
    public void delete() {
        protocol.delete();
    }
}
