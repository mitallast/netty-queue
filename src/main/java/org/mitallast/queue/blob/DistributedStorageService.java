package org.mitallast.queue.blob;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.blob.protocol.*;
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

public class DistributedStorageService {
    private final Logger logger = LogManager.getLogger();
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
        this.discovery = discovery;
        this.transportService = transportService;
        this.blobStorageService = blobStorageService;
        this.raft = raft;
        this.fsm = fsm;
        this.QoS = config.getInt("blob.QoS");

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
            try {
                transportService.connectToNode(node);
                transportService.channel(node).send(message);
            } catch (IOException e) {
                logger.error("error send message to {}", node);
            }
        }
        return future;
    }

    public CompletableFuture<GetBlobResourceResponse> getResource(String key) {
        logger.info("value resource {}", key);
        CompletableFuture<GetBlobResourceResponse> future = new CompletableFuture<>();
        ImmutableMap<String, ImmutableSet<DiscoveryNode>> routingMap = fsm.getRoutingMap().getRoutingMap();
        if (routingMap.containsKey(key)) {
            ImmutableSet<DiscoveryNode> nodes = routingMap.get(key);

            long id = requestId.incrementAndGet();
            requests.put(id, future);

            DiscoveryNode node = nodes.asList().get((int) (id % nodes.size()));

            logger.info("value resource {} id {} node {}", key, id, node);
            try {
                transportService.connectToNode(node);
                transportService.channel(node).send(new GetBlobResourceRequest(node, id, key));
            } catch (IOException e) {
                logger.error("error send message to {}", node);
            }
        } else {
            future.completeExceptionally(new RuntimeException("resource not found"));
        }
        return future;
    }

    private void handle(GetBlobResourceRequest message) {
        try {
            logger.info("handle value resource request {} id {}", message.getKey(), message.getId());
            InputStream stream = blobStorageService.getObject(message.getKey());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] bytes = new byte[4096];
            int read;
            while ((read = stream.read(bytes)) > 0) {
                out.write(bytes, 0, read);
            }
            GetBlobResourceResponse response = new GetBlobResourceResponse(message.getId(), message.getKey(), out.toByteArray());
            transportService.connectToNode(message.getNode());
            transportService.channel(message.getNode()).send(response);
        } catch (IOException e) {
            logger.warn("error value resource {}: {}", message.getKey(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private void handle(GetBlobResourceResponse message) {
        logger.info("handle value resource response {} id {}", message.getKey(), message.getId());
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
            raft.apply(new ClientMessage(message.getNode(), cmd));
        } else {
            try {
                transportService.connectToNode(message.getNode());
                transportService.channel(message.getNode()).send(new PutBlobResourceResponse(
                    message.getId(), message.getKey(),
                    false
                ));
            } catch (IOException e) {
                logger.error("error send message to {}: ", message.getNode());
            }
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
