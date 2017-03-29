package org.mitallast.queue.blob.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.queue.blob.DistributedStorageFSM;
import org.mitallast.queue.blob.DistributedStorageService;
import org.mitallast.queue.blob.protocol.GetBlobResourceResponse;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class BlobHandler {

    private final DistributedStorageService storageService;
    private final DistributedStorageFSM fsm;

    @Inject
    public BlobHandler(RestController controller, DistributedStorageService storageService, DistributedStorageFSM fsm) {
        this.storageService = storageService;
        this.fsm = fsm;
        controller.handler(this::put)
            .param(controller.param().string("key"))
            .param(controller.param().content())
            .response(controller.response().futureJson())
            .handle(HttpMethod.POST, HttpMethod.PUT, "_blob/{key}");

        controller.handler(this::get)
            .param(controller.param().string("key"))
            .response(controller.response().futureBytes())
            .handle(HttpMethod.GET, "_blob/{key}");

        controller.handler(this::resources)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "_blob");
    }

    public CompletableFuture<ImmutableMap<String, Boolean>> put(String key, ByteBuf content) {
        int size = content.readableBytes();
        byte[] data = new byte[size];
        content.readBytes(data);
        content.release();

        return storageService.putResource(key, data)
            .thenApply(stored -> ImmutableMap.of("stored", stored));
    }

    public CompletableFuture<byte[]> get(String key) {
        return storageService.getResource(key)
            .thenApply(GetBlobResourceResponse::getData);
    }

    public ImmutableSet<Map.Entry<String, ImmutableSet<DiscoveryNode>>> resources() {
        return fsm.getRoutingMap().getRoutingMap().entrySet();
    }
}
