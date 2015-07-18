package org.mitallast.queue.raft;

import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.log.LogEntryGenerator;
import org.mitallast.queue.log.entry.LogEntry;
import org.mitallast.queue.raft.resource.ResourceService;
import org.mitallast.queue.raft.resource.structures.AsyncBoolean;
import org.mitallast.queue.raft.resource.structures.LogResource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RaftBenchmark extends BaseRaftTest {

    private ResourceService resourceService;
    private AsyncBoolean asyncBoolean;

    @Override
    protected int max() {
        return 100000;
    }

    @Before
    public void setUpResource() throws Exception {
        resourceService = leader.injector().getInstance(ResourceService.class);
        asyncBoolean = resourceService.create("boolean", AsyncBoolean.class).get();
    }

    @Test
    public void benchAsyncBoolean() throws Exception {
        long start = System.currentTimeMillis();
        ArrayList<CompletableFuture<Boolean>> futures = new ArrayList<>(max());
        for (int i = 0; i < max(); i++) {
            CompletableFuture<Boolean> future = asyncBoolean.getAndSet(i % 2 == 0);
            futures.add(future);
        }
        for (CompletableFuture<Boolean> future : futures) {
            try {
                future.get();
            } catch (Throwable e) {
                logger.warn("error", e);
            }
        }
        long end = System.currentTimeMillis();
        printQps("async boolean", max(), start, end);
    }

    @Test
    public void benchAppend() throws Exception {
        LogEntry[] entries = LogEntryGenerator.generate(max());
        List<CompletableFuture<Long>> futures = new ArrayList<>(max());

        LogResource logResource = resourceService.create("log", LogResource.class).get();

        long start = System.currentTimeMillis();
        for (LogEntry entry : entries) {
            CompletableFuture<Long> future = logResource.appendEntry(entry);
            futures.add(future);
        }
        for (CompletableFuture<Long> future : futures) {
            future.get();
        }
        long end = System.currentTimeMillis();
        printQps("log append", max(), start, end);
    }
}
