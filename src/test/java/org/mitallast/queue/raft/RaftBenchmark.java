package org.mitallast.queue.raft;

import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.resource.ResourceService;
import org.mitallast.queue.raft.resource.structures.AsyncBoolean;
import org.mitallast.queue.raft.state.RaftStateContext;
import org.mitallast.queue.raft.state.RaftStateType;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RaftBenchmark extends BaseIntegrationTest {

    private static final int max = 100000;
    private InternalNode[] nodes;
    private InternalNode leader = null;
    private ResourceService resourceService;
    private AsyncBoolean asyncBoolean;

    @Before
    public void setUp() throws Exception {
        int[] ports = new int[3];
        InternalNode[] nodes = new InternalNode[ports.length];

        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < ports.length; i++) {
            ports[i] = random.nextInt(10000) + 20000;
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append("127.0.0.1:").append(ports[i]);
        }
        String nodesString = buffer.toString();

        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(ImmutableSettings.builder()
                .put(settings())
                .put("transport.port", ports[i])
                .put("raft.cluster.nodes", nodesString)
                .build());
        }
        // await for leader election
        do {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            for (InternalNode node : nodes) {
                if (node.injector().getInstance(RaftStateContext.class).getState() == RaftStateType.LEADER) {
                    leader = node;
                    resourceService = leader.injector().getInstance(ResourceService.class);
                    asyncBoolean = resourceService.create("boolean", AsyncBoolean.class).get();
                    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                    break;
                }
            }
        } while (leader == null);

    }

    @Test
    public void benchAsyncBoolean() throws Exception {
        long start = System.currentTimeMillis();
        ArrayList<CompletableFuture<Boolean>> futures = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
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
        printQps("async boolean", max, start, end);
    }
}
