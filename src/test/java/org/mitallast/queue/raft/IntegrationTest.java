package org.mitallast.queue.raft;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.log.entry.TextLogEntry;
import org.mitallast.queue.raft.resource.Node;
import org.mitallast.queue.raft.resource.ResourceService;
import org.mitallast.queue.raft.resource.structures.AsyncBoolean;
import org.mitallast.queue.raft.resource.structures.AsyncMap;
import org.mitallast.queue.raft.resource.structures.LogResource;
import org.mitallast.queue.raft.util.StringValue;

public class IntegrationTest extends BaseRaftTest {
    @Test
    @SuppressWarnings("unchecked")
    public void test() throws Exception {
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
