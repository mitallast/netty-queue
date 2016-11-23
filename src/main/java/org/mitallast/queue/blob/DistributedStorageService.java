package org.mitallast.queue.blob;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.blob.protocol.*;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class DistributedStorageService extends AbstractComponent {

    private final ClusterDiscovery discovery;
    private final TransportService transportService;
    private final BlobStorageService blobStorageService;
    private final Raft raft;
    private final DistributedStorageFSM fsm;
    private final int QoS;
    private final ConcurrentMap<Long, CompletableFuture> requests = new ConcurrentHashMap<>();
    private final AtomicLong requestId = new AtomicLong();

    @Inject
    public DistributedStorageService(
        Config config,
        TransportController transportController,
        ClusterDiscovery discovery,
        TransportService transportService,
        BlobStorageService blobStorageService,
        Raft raft,
        DistributedStorageFSM fsm
    ) {
        super(config.getConfig("blob"), DistributedStorageService.class);
        this.discovery = discovery;
        this.transportService = transportService;
        this.blobStorageService = blobStorageService;
        this.raft = raft;
        this.fsm = fsm;
        this.QoS = this.config.getInt("QoS");

        transportController.<PutBlobResourceRequest>registerMessageHandler(PutBlobResourceRequest.class, this::handle);
        transportController.<PutBlobResourceResponse>registerMessageHandler(PutBlobResourceResponse.class, this::handle);
        transportController.<GetBlobResourceRequest>registerMessageHandler(GetBlobResourceRequest.class, this::handle);
        transportController.<GetBlobResourceResponse>registerMessageHandler(GetBlobResourceResponse.class, this::handle);
    }

    public CompletableFuture<Boolean> putResource(String key, byte[] data) {
        logger.info("put resource: {}, bytes: {}", key, data.length);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        List<DiscoveryNode> replicas = new ArrayList<>(raft.currentMeta().getConfig().members());
        for (int i = 0, max = replicas.size() - 1; i < max; i++) {
            int swap = new Random().nextInt(replicas.size() - max) + 1;
            DiscoveryNode a = replicas.get(i);
            DiscoveryNode b = replicas.get(swap);
            replicas.set(i, b);
            replicas.set(swap, a);
        }
        BiConsumer<PutBlobResourceResponse, Throwable> completeListener = new BiConsumer<PutBlobResourceResponse, Throwable>() {
            private AtomicLong countDown = new AtomicLong(QoS);
            private boolean stored = false;

            @Override
            public void accept(PutBlobResourceResponse response, Throwable throwable) {
                stored = stored || response.isStored();
                long count = countDown.decrementAndGet();
                logger.info("handle complete: {} stored: {}", count, stored);
                if (count == 0) {
                    future.complete(stored);
                }
            }
        };
        for (int i = 0; i < QoS; i++) {
            CompletableFuture<PutBlobResourceResponse> nodeFuture = new CompletableFuture<>();
            long id = requestId.incrementAndGet();
            requests.put(id, nodeFuture);
            nodeFuture.whenComplete(completeListener);

            DiscoveryNode node = replicas.get(i);
            logger.info("send put request {} to {}", id, node);

            PutBlobResourceRequest message = new PutBlobResourceRequest(discovery.self(), id, key, data);
            transportService.connectToNode(node);
            transportService.channel(node).message(message);
        }
        return future;
    }

    public CompletableFuture<GetBlobResourceResponse> getResource(String key) {
        logger.info("get resource {}", key);
        CompletableFuture<GetBlobResourceResponse> future = new CompletableFuture<>();
        ImmutableMap<String, ImmutableSet<DiscoveryNode>> routingMap = fsm.getRoutingMap().getRoutingMap();
        if (routingMap.containsKey(key)) {
            ImmutableSet<DiscoveryNode> nodes = routingMap.get(key);

            long id = requestId.incrementAndGet();
            requests.put(id, future);

            DiscoveryNode node = nodes.asList().get((int) (id % nodes.size()));

            logger.info("get resource {} id {} node {}", key, id, node);
            transportService.connectToNode(node);
            transportService.channel(node).message(new GetBlobResourceRequest(node, id, key));
        } else {
            future.completeExceptionally(new RuntimeException("resource not found"));
        }
        return future;
    }

    private void handle(GetBlobResourceRequest message) {
        try {
            logger.info("handle get resource request {} id {}", message.getKey(), message.getId());
            InputStream stream = blobStorageService.getObject(message.getKey());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] bytes = new byte[4096];
            int read;
            while ((read = stream.read(bytes)) > 0) {
                out.write(bytes, 0, read);
            }
            transportService.connectToNode(message.getNode());
            transportService.channel(message.getNode())
                .message(new GetBlobResourceResponse(message.getId(), message.getKey(), out.toByteArray()));
        } catch (IOException e) {
            logger.warn("error get resource {}: {}", message.getKey(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private void handle(GetBlobResourceResponse message) {
        logger.info("handle get resource response {} id {}", message.getKey(), message.getId());
        CompletableFuture completableFuture = requests.get(message.getId());
        if (completableFuture != null) {
            completableFuture.complete(message);
        }
    }

    private void handle(PutBlobResourceRequest message) {
        logger.info("handle put request: {}", message.getId());
        boolean stored = false;
        try {
            blobStorageService.putObject(message.getKey(), new ByteArrayInputStream(message.getData()));
            stored = true;
        } catch (IOException e) {
            logger.error("error store: {}", e);
        }

        if (stored) {
            PutBlobResource cmd = new PutBlobResource(
                discovery.self(),
                message.getId(),
                message.getKey()
            );
            raft.receive(new ClientMessage(message.getNode(), cmd));
        } else {
            transportService.connectToNode(message.getNode());
            transportService.channel(message.getNode()).message(new PutBlobResourceResponse(
                message.getId(), message.getKey(),
                false
            ));
        }
    }

    @SuppressWarnings("unchecked")
    private void handle(PutBlobResourceResponse message) {
        CompletableFuture completableFuture = requests.get(message.getId());
        logger.info("handle put response: {}", message.getId());
        if (completableFuture != null) {
            logger.info("complete put future: {}", message.getId());
            completableFuture.complete(message);
        }
    }
}
