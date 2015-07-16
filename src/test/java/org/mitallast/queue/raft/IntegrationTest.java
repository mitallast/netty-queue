package org.mitallast.queue.raft;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.log.entry.TextLogEntry;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.log.RaftLog;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;
import org.mitallast.queue.raft.resource.Node;
import org.mitallast.queue.raft.resource.ResourceService;
import org.mitallast.queue.raft.resource.structures.AsyncBoolean;
import org.mitallast.queue.raft.resource.structures.AsyncMap;
import org.mitallast.queue.raft.resource.structures.LogResource;
import org.mitallast.queue.raft.state.RaftStateContext;
import org.mitallast.queue.raft.state.RaftStateType;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.raft.util.StringValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class IntegrationTest extends BaseIntegrationTest {

    @Test
    @SuppressWarnings("unchecked")
    public void test() throws Exception {
        int port1 = random.nextInt(10000) + 20000;
        int port2 = random.nextInt(10000) + 20000;
        InternalNode node1 = createNode(ImmutableSettings.builder()
            .put(settings())
            .put("transport.port", port1)
            .put("raft.cluster.nodes", "127.0.0.1:" + port1 + ", 127.0.0.1:" + port2)
            .build());
        InternalNode node2 = createNode(ImmutableSettings.builder()
            .put(settings())
            .put("transport.port", port2)
            .put("raft.cluster.nodes", "127.0.0.1:" + port1 + ", 127.0.0.1:" + port2)
            .build());

        // await for leaders
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        logger.info("node1 is {}", node1.injector().getInstance(RaftStateContext.class).getState());
        logger.info("node2 is {}", node2.injector().getInstance(RaftStateContext.class).getState());

        node1.injector().getInstance(ExecutionContext.class).submit(() -> {
            RaftLog log = node1.injector().getInstance(RaftLog.class);
            logger.info("log first index {}", log.firstIndex());
            logger.info("log last index {}", log.lastIndex());
            for (long i = log.firstIndex(); i < log.lastIndex(); i++) {
                try {
                    RaftLogEntry entry = log.getEntry(i);
                    logger.info("[{}] {}", i, entry);
                } catch (IOException e) {
                    logger.error("error", e);
                }
            }
        }).get();

        logger.info("await for leader election");

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        logger.info("node1 is {}", node1.injector().getInstance(RaftStateContext.class).getState());
        logger.info("node2 is {}", node2.injector().getInstance(RaftStateContext.class).getState());

        final InternalNode leader;

        if (node1.injector().getInstance(RaftStateContext.class).getState().equals(RaftStateType.LEADER)) {
            Assert.assertEquals(RaftStateType.FOLLOWER, node2.injector().getInstance(RaftStateContext.class).getState());
            leader = node1;
        } else {
            Assert.assertEquals(RaftStateType.FOLLOWER, node1.injector().getInstance(RaftStateContext.class).getState());
            Assert.assertEquals(RaftStateType.LEADER, node2.injector().getInstance(RaftStateContext.class).getState());
            leader = node2;
        }

        // test base resource

        ResourceService resourceService = leader.injector().getInstance(ResourceService.class);

        Node foo = resourceService.create("foo").get();
        Assert.assertNotNull(foo);

        Boolean fooExists = resourceService.exists("foo").get();
        Assert.assertNotNull(fooExists);
        Assert.assertTrue(fooExists);

        Boolean fooDeleted = resourceService.delete("foo").get();
        Assert.assertNotNull(fooDeleted);
        Assert.assertTrue(fooDeleted);

        Boolean fooExists2 = resourceService.exists("foo").get();
        Assert.assertNotNull(fooExists2);
        Assert.assertFalse(fooExists2);

        // test async boolean resource

        AsyncBoolean asyncBoolean = resourceService.create("boolean", AsyncBoolean.class).get();

        Assert.assertFalse(asyncBoolean.get().get());
        Assert.assertTrue(asyncBoolean.compareAndSet(false, true).get());
        Assert.assertTrue(asyncBoolean.get().get());
        asyncBoolean.set(false).get();
        Assert.assertFalse(asyncBoolean.get().get());
        Assert.assertFalse(asyncBoolean.getAndSet(true).get());

        AsyncMap<StringValue, StringValue> asyncMap = resourceService.create("map", AsyncMap.class).get();

        StringValue bar = new StringValue("bar");
        StringValue baz = new StringValue("baz");
        Assert.assertFalse(asyncMap.containsKey(bar).get());
        Assert.assertNull(asyncMap.put(bar, baz).get());
        Assert.assertTrue(asyncMap.containsKey(bar).get());

        LogResource logResource = resourceService.create("log", LogResource.class).get();

        Long index = logResource.appendEntry(TextLogEntry.builder().setMessage("hello world").build()).get();
        Assert.assertNotNull(index);

        logger.info("close nodes");
    }
}
