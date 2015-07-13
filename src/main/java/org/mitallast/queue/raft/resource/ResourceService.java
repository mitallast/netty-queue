package org.mitallast.queue.raft.resource;

import com.google.inject.Inject;
import org.mitallast.queue.raft.Protocol;
import org.mitallast.queue.raft.StateMachine;
import org.mitallast.queue.raft.resource.manager.*;
import org.mitallast.queue.raft.resource.result.BooleanResult;
import org.mitallast.queue.raft.resource.result.StringListResult;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceService {
    public static final String PATH_SEPARATOR = "/";
    protected final Protocol protocol;
    private final Map<String, Node> nodes = new ConcurrentHashMap<>();

    @Inject
    public ResourceService(Protocol protocol) {
        this.protocol = protocol;
    }

    protected Node node(String path) {
        if (path == null)
            throw new NullPointerException("path cannot be null");
        if (!path.startsWith(PATH_SEPARATOR))
            path = PATH_SEPARATOR + path;
        if (path.endsWith(PATH_SEPARATOR))
            path = path.substring(0, path.length() - 1);
        return nodes.computeIfAbsent(path, p -> new Node(p, this));
    }

    public CompletableFuture<Boolean> exists(String path) {
        return protocol.submit(new PathExists(path)).thenApply(BooleanResult::get);
    }

    public CompletableFuture<Node> create(String path) {
        return protocol.submit(CreatePath.builder()
            .setPath(path)
            .build())
            .thenApply(result -> node(path));
    }

    public CompletableFuture<List<String>> children(String path) {
        return protocol.submit(PathChildren.builder()
            .setPath(path)
            .build())
            .thenApply(StringListResult::get);
    }

    public <T extends Resource> CompletableFuture<T> create(String path, Class<? extends T> type) {
        return protocol.submit(CreateResource.builder()
            .setPath(path)
            .setType(lookup(type))
            .build())
            .thenApply(id -> createResource(type, id.get()));
    }

    private Class<? extends StateMachine> lookup(Class<? extends Resource> resourceType) {
        Stateful stateful = resourceType.getAnnotation(Stateful.class);
        if (stateful != null) {
            return stateful.value();
        } else {
            throw new IllegalArgumentException("unknown resource state: " + resourceType);
        }
    }

    public CompletableFuture<Boolean> delete(String path) {
        return protocol.submit(DeletePath.builder()
            .setPath(path)
            .build())
            .thenApply(BooleanResult::get);
    }

    @SuppressWarnings("unchecked")
    private <T extends Resource> T createResource(Class<? extends T> type, long id) {
        try {
            Constructor constructor = type.getConstructor(Protocol.class);
            return (T) constructor.newInstance(new ResourceProtocol(id, protocol));
        } catch (Exception e) {
            throw new ResourceException("failed to instantiate resource: " + type, e);
        }
    }

}
