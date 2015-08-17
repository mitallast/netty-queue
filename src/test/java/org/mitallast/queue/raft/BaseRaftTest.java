package org.mitallast.queue.raft;

import org.junit.Before;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.state.RaftStateContext;
import org.mitallast.queue.raft.state.RaftStateType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BaseRaftTest extends BaseIntegrationTest {

    protected InternalNode[] nodes;
    protected InternalNode leader;

    protected int nodeCount() {
        return 2;
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

        List<Future<InternalNode>> futures = new ArrayList<>(ports.length);
        for (int i = 0; i < nodes.length; i++) {
            final int node = i;
            Future<InternalNode> future = submit(() -> nodes[node] = createNode(ImmutableSettings.builder()
                .put(settings())
                .put("transport.port", ports[node])
                .put("raft.cluster.nodes", nodesString)
                .build()));
            futures.add(future);
        }

        for (Future<InternalNode> future : futures) {
            future.get();
        }

        // await for leader election
        do {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            for (InternalNode node : nodes) {
                if (node.injector().getInstance(RaftStateContext.class).getState() == RaftStateType.LEADER) {
                    leader = node;
                    break;
                }
            }
        } while (leader == null);
    }
}
