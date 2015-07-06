package org.mitallast.queue.raft.resource;

import com.google.common.base.Preconditions;
import org.mitallast.queue.raft.resource.manager.PathChildren;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Node {
    private final String path;
    private final ResourceService resourceService;

    public Node(String path, ResourceService resourceService) {
        Preconditions.checkNotNull(path);
        Preconditions.checkNotNull(resourceService);
        this.path = path;
        this.resourceService = resourceService;
    }

    public String path() {
        return path;
    }

    public Node node(String path) {
        if (path == null)
            throw new NullPointerException("path cannot be null");
        if (!path.startsWith(ResourceService.PATH_SEPARATOR))
            path = ResourceService.PATH_SEPARATOR + path;
        if (path.endsWith(ResourceService.PATH_SEPARATOR))
            path = path.substring(0, path.length() - 1);
        return resourceService.node(this.path + path);
    }

    public CompletableFuture<List<Node>> children() {
        return resourceService.protocol.submit(PathChildren.builder()
            .setPath(path)
            .build())
            .thenApply(children ->
                children.get().stream()
                    .map(resourceService::node)
                    .collect(Collectors.toList()));
    }

    public CompletableFuture<Boolean> exists() {
        return resourceService.exists(path);
    }

    public CompletableFuture<Boolean> exists(String path) {
        return resourceService.exists(String.format("%s%s%s", this.path, ResourceService.PATH_SEPARATOR, path));
    }

    public CompletableFuture<Node> create() {
        return resourceService.create(path);
    }

    public CompletableFuture<Node> create(String path) {
        return resourceService.create(String.format("%s%s%s", this.path, ResourceService.PATH_SEPARATOR, path));
    }

    public <T extends Resource> CompletableFuture<T> create(Class<? extends T> type) {
        return resourceService.create(path, type);
    }

    public <T extends Resource> CompletableFuture<T> get(Class<T> type) {
        return resourceService.create(path, type);
    }

    public CompletableFuture<Void> delete() {
        return resourceService.delete(path).thenApply(result -> null);
    }

    public CompletableFuture<Void> delete(String path) {
        return resourceService.delete(String.format("%s%s%s", this.path, ResourceService.PATH_SEPARATOR, path)).thenApply(result -> null);
    }

}
