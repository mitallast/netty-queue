package org.mitallast.queue.benchmark;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.transport.TransportController;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class BenchmarkService extends AbstractComponent {

    private final ClusterDiscovery discovery;
    private final Raft raft;
    private final ConcurrentMap<Long, CompletableFuture<BenchmarkResponse>> requests = new ConcurrentHashMap<>();
    private final AtomicLong requestId = new AtomicLong();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Inject
    public BenchmarkService(
        Config config,
        TransportController transportController,
        ClusterDiscovery discovery,
        Raft raft
    ) {
        super(config, BenchmarkService.class);

        this.discovery = discovery;
        this.raft = raft;

        transportController.registerMessageHandler(BenchmarkResponse.class, this::handle);
    }

    public CompletableFuture<BenchmarkResult> benchmark(int requests, int dataSize) {
        logger.info("start benchmark: requests={} data size={}", requests, dataSize);

        byte[] bytes = new byte[dataSize];
        new Random().nextBytes(bytes);

        final long start = System.currentTimeMillis();
        final CompletableFuture<BenchmarkResult> resultFuture = new CompletableFuture<>();
        final BiConsumer<BenchmarkResponse, Throwable> handler = new BiConsumer<BenchmarkResponse, Throwable>() {
            private final AtomicLong counter = new AtomicLong(requests);

            @Override
            public void accept(BenchmarkResponse response, Throwable throwable) {
                if (counter.decrementAndGet() == 0) {
                    long end = System.currentTimeMillis();
                    logger.info("finish benchmark: requests={} data size={} at {}ms", requests, dataSize, end - start);
                    resultFuture.complete(new BenchmarkResult(
                        requests, dataSize,
                        start, end
                    ));
                }
            }
        };
        executorService.execute(() -> {
            for (int i = 0; i < requests; i++) {
                CompletableFuture<BenchmarkResponse> future = send(bytes);
                future.whenComplete(handler);
            }
        });
        return resultFuture;
    }

    private CompletableFuture<BenchmarkResponse> send(byte[] data) {
        long request = requestId.incrementAndGet();
        CompletableFuture<BenchmarkResponse> future = new CompletableFuture<>();
        requests.put(request, future);
        raft.apply(new ClientMessage(discovery.self(), new BenchmarkRequest(request, data)));
        return future;
    }

    private void handle(BenchmarkResponse response) {
        CompletableFuture<BenchmarkResponse> completableFuture = requests.remove(response.getRequest());
        if (completableFuture != null) {
            completableFuture.complete(response);
        }
    }
}
