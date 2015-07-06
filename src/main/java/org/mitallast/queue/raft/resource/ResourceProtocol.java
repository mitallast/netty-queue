package org.mitallast.queue.raft.resource;

import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.Protocol;
import org.mitallast.queue.raft.Query;
import org.mitallast.queue.raft.resource.manager.DeleteResource;

import java.util.concurrent.CompletableFuture;

public class ResourceProtocol implements Protocol {
    private final long resource;
    private final Protocol protocol;

    public ResourceProtocol(long resource, Protocol protocol) {
        this.resource = resource;
        this.protocol = protocol;
    }

    @Override
    public <T extends Streamable> CompletableFuture<T> submit(Command<T> command) {
        return protocol.submit(ResourceCommand.<Command<T>, T>builder()
            .setResource(resource)
            .setCommand(command)
            .build());
    }

    @Override
    public <T extends Streamable> CompletableFuture<T> submit(Query<T> query) {
        return protocol.submit(ResourceQuery.<Query<T>, T>builder()
            .setResource(resource)
            .setQuery(query)
            .build());
    }

    @Override
    public void delete() {
        protocol.submit(DeleteResource.builder()
            .setResource(resource)
            .build())
            .thenApply(deleted -> null);
    }
}
