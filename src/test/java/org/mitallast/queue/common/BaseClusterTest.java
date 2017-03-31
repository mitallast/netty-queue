package org.mitallast.queue.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.ClusterRaftTest;
import org.mitallast.queue.raft.Raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mitallast.queue.raft.RaftState.Follower;
import static org.mitallast.queue.raft.RaftState.Leader;

public abstract class BaseClusterTest extends BaseTest {
    protected static final int nodesCount = 3;

    protected ImmutableList<InternalNode> nodes;
    protected ImmutableList<Raft> raft;

    @Before
    public void setUpNodes() throws Exception {
        HashSet<Integer> ports = new HashSet<>();
        while (ports.size() < nodesCount) {
            ports.add(8800 + random.nextInt(99));
        }

        String nodeDiscovery = ports.stream().map(port -> "127.0.0.1:" + port).reduce((a, b) -> a + "," + b).orElse("");

        ImmutableList.Builder<InternalNode> builder = ImmutableList.builder();
        boolean bootstrap = true;
        for (Integer port : ports) {
            Config config = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
                .put("nodes.name", "nodes" + port)
                .put("nodes.path", testFolder.newFolder().getAbsolutePath())
                .put("rest.enabled", false)
                .put("raft.discovery.host", "127.0.0.1")
                .put("raft.discovery.port", port)
                .put("raft.discovery.nodes.0", nodeDiscovery)
                .put("raft.keep-init-until-found", nodesCount)
                .put("raft.election-deadline", "1s")
                .put("raft.heartbeat", "500ms")
                .put("raft.bootstrap", bootstrap)
                .put("raft.snapshot-interval", 10000)
                .put("transport.host", "127.0.0.1")
                .put("transport.port", port)
                .put("transport.max_connections", 1)
                .build());
            bootstrap = false;
            builder.add(new InternalNode(config, testModules()));
        }
        nodes = builder.build();
        raft = ImmutableList.copyOf(nodes.stream().map(node -> node.injector().getInstance(Raft.class)).iterator());

        ArrayList<Future> futures = new ArrayList<>();
        for (InternalNode item : this.nodes) {
            Future future = submit(() -> {
                try {
                    item.start();
                } catch (IOException e) {
                    assert false : e;
                }
            });
            futures.add(future);
        }
        for (Future future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
    }

    @After
    public void tearDownNodes() throws Exception {
        for (InternalNode node : this.nodes) {
            node.stop();
        }
        for (InternalNode node : this.nodes) {
            node.close();
        }
    }

    protected AbstractModule[] testModules() {
        return new AbstractModule[0];
    }

    protected final void awaitElection() throws Exception {
        while (true) {
            if (raft.stream().anyMatch(raft -> raft.currentState() == Leader)) {
                if (raft.stream().allMatch(raft -> raft.replicatedLog().committedIndex() == nodesCount)) {
                    logger.info("leader found, cluster available");
                    return;
                }
            }
            Thread.sleep(10);
        }
    }

    protected final void assertLeaderElected() throws Exception {
        Assert.assertEquals(1, raft.stream().filter(raft -> raft.currentState() == Leader).count());
        Assert.assertEquals(nodesCount - 1, raft.stream().filter(raft -> raft.currentState() == Follower).count());
    }
}
