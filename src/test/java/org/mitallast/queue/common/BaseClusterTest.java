package org.mitallast.queue.common;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import javaslang.collection.HashMap;
import javaslang.collection.HashSet;
import javaslang.collection.Map;
import javaslang.collection.Vector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.Raft;

import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mitallast.queue.raft.RaftState.Follower;
import static org.mitallast.queue.raft.RaftState.Leader;

public abstract class BaseClusterTest extends BaseTest {
    protected static final int nodesCount = 3;

    protected Vector<InternalNode> nodes;
    protected Vector<Raft> raft;

    private Vector<Integer> ports() {
        HashSet<Integer> ports = HashSet.empty();
        while (ports.size() < nodesCount) {
            ports = ports.add(8800 + random.nextInt(99));
        }
        return ports.toVector();
    }

    @Before
    public void setUpNodes() throws Exception {
        Vector<Integer> ports = ports();
        assert ports.size() == nodesCount;

        String nodeDiscovery = ports.map(port -> "127.0.0.1:" + port).reduce((a, b) -> a + "," + b);

        String path = testFolder.newFolder().getAbsolutePath();
        nodes = ports.map(port -> {
            Config config = ConfigFactory.parseMap(HashMap.ofEntries(
                Map.entry("node.path", path),
                Map.entry("rest.enabled", false),
                Map.entry("raft.discovery.host", "127.0.0.1"),
                Map.entry("raft.discovery.port", port),
                Map.entry("raft.discovery.nodes.0", nodeDiscovery),
                Map.entry("raft.keep-init-until-found", nodesCount),
                Map.entry("raft.election-deadline", "1s"),
                Map.entry("raft.heartbeat", "500ms"),
                Map.entry("raft.bootstrap", port.equals(ports.head())),
                Map.entry("raft.snapshot-interval", 10000),
                Map.entry("crdt.buckets", nodesCount),
                Map.entry("crdt.replicas", nodesCount),
                Map.entry("transport.host", "127.0.0.1"),
                Map.entry("transport.port", port),
                Map.entry("transport.max_connections", 1)
            ).toJavaMap());
            return new InternalNode(config, testModules());
        });
        raft = nodes.map(node -> node.injector().getInstance(Raft.class));

        ArrayList<Future> futures = new ArrayList<>();
        for (InternalNode item : this.nodes) {
            Future future = submit(item::start);
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
            if (raft.exists(raft -> raft.currentState() == Leader)) {
                logger.warn("leader found");
                if (raft.forAll(raft -> !raft.currentMeta().getConfig().isTransitioning())) {
                    if (raft.forAll(raft -> raft.currentMeta().getConfig().members().size() == nodesCount)) {
                        logger.info("leader found, cluster available");
                        return;
                    } else {
                        logger.warn("nodes not synced");
                        raft.map(r -> r.currentMeta().getConfig().members()).forEach(m -> logger.info("members: {}", m));
                    }
                } else {
                    logger.warn("nodes in joint");
                }
            } else {
                logger.warn("no leader found");
            }
            Thread.sleep(1000);
        }
    }

    protected final void assertLeaderElected() throws Exception {
        Assert.assertEquals(1, raft.count(raft -> raft.currentState() == Leader));
        Assert.assertEquals(nodesCount - 1, raft.count(raft -> raft.currentState() == Follower));
    }
}
