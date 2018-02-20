package org.mitallast.queue.common;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import io.vavr.collection.Vector;
import org.junit.After;
import org.junit.Assert;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.Raft;

import java.io.IOException;

import static org.mitallast.queue.raft.RaftState.Follower;
import static org.mitallast.queue.raft.RaftState.Leader;

public abstract class BaseClusterTest extends BaseTest {

    protected Vector<InternalNode> nodes = Vector.empty();
    private Set<Integer> ports = HashSet.empty();
    private int leaderPort;

    private int nextPort() {
        for (; ; ) {
            int port = 8800 + random.nextInt(100);
            if (!ports.contains(port)) {
                ports = ports.add(port);
                return port;
            }
        }
    }

    protected ConfigBuilder config() throws IOException {
        String path = testFolder.newFolder().getAbsolutePath();
        return new ConfigBuilder()
            .with("node.path", path)
            .with("rest.enabled", false)
            .with("raft.discovery.host", "127.0.0.1")
            .with("transport.host", "127.0.0.1");
    }

    protected final void createLeader() throws Exception {
        assert ports.isEmpty() : "ports not empty";
        leaderPort = nextPort();
        Config config = config()
            .with("raft.discovery.port", leaderPort)
            .with("raft.bootstrap", true)
            .with("transport.port", leaderPort)
            .build();
        InternalNode node = new InternalNode(config, testModules());
        node.start();
        nodes = nodes.append(node);
    }

    protected final void createFollower() throws Exception {
        assert ports.nonEmpty() : "ports not empty";
        int port = nextPort();
        Config config = config()
            .with("raft.discovery.port", port)
            .with("raft.discovery.nodes.0", "127.0.0.1:" + leaderPort)
            .with("raft.bootstrap", false)
            .with("transport.port", port)
            .build();
        InternalNode node = new InternalNode(config, testModules());
        node.start();
        nodes = nodes.append(node);
    }

    @After
    public void tearDownNodes() throws Exception {
        for (InternalNode node : nodes) {
            node.stop();
        }
        for (InternalNode node : nodes) {
            node.close();
        }
        nodes = Vector.empty();
        ports = HashSet.empty();
    }

    protected AbstractModule[] testModules() {
        return new AbstractModule[0];
    }

    protected final void awaitElection() throws Exception {
        Vector<Raft> raft = nodes.map(node -> node.injector().getInstance(Raft.class));
        while (true) {
            if (raft.exists(r -> r.currentState() == Leader)) {
                logger.warn("leader found");
                if (raft.forAll(r -> !r.currentMeta().getConfig().isTransitioning())) {
                    logger.info("leader found");
                    if (raft.forAll(r -> r.currentMeta().getConfig().getMembers().size() == raft.size())) {
                        logger.info("cluster available");
                        return;
                    } else {
                        logger.warn("nodes {} not synced", raft.size());
                        raft.map(r -> r.currentMeta().getConfig().getMembers()).forEach(m -> logger.info("members: {}", m));
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
        Vector<Raft> raft = nodes.map(node -> node.injector().getInstance(Raft.class));
        Assert.assertEquals(1, raft.count(r -> r.currentState() == Leader));
        Assert.assertEquals(raft.size() - 1, raft.count(r -> r.currentState() == Follower));
    }
}
