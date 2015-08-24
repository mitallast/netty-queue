package org.mitallast.queue.raft;

import org.junit.Before;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.state.RaftStateContext;
import org.mitallast.queue.raft.state.RaftStateType;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BaseRaftTest extends BaseIntegrationTest {

    protected InternalNode[] nodes;
    protected InternalNode leader;

    protected int nodeCount() {
        return 3;
    }

    @Before
    public void setUpNodes() throws Exception {
        int[] ports = new int[nodeCount()];
        nodes = new InternalNode[ports.length];

        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < ports.length; i++) {
            ports[i] = random.nextInt(10000) + 20000;
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append("127.0.0.1:").append(ports[i]);
        }
        String nodesString = buffer.toString();

        List<Future> futures = IntStream.range(0, nodeCount())
            .mapToObj(node -> submit(() -> {
                logger.info("starting node {}", node);
                try {
                    nodes[node] = createNode(ImmutableSettings.builder()
                        .put(settings())
                        .put("transport.port", ports[node])
                        .put("raft.cluster.nodes", nodesString)
                        .build());
                } catch (Throwable e) {
                    logger.error("error start node {}", node, e);
                    assert false : e;
                } finally {
                    logger.info("starting node {} done", node);
                }
            })).collect(Collectors.toList());
        for (Future future : futures) {
            future.get(1, TimeUnit.MINUTES);
        }

        // await for leader election
        do {
            Thread.sleep(10);
            for (InternalNode node : nodes) {
                if (node.injector().getInstance(RaftStateContext.class).getState() == RaftStateType.LEADER) {
                    leader = node;
                    break;
                }
            }
        } while (leader == null);
    }
}
