package org.mitallast.queue.raft.persistent;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import javaslang.collection.HashMap;
import javaslang.control.Option;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Collections;

public class PersistentTest extends BaseTest {

    private final long term1 = 1;
    private final DiscoveryNode node = new DiscoveryNode("127.0.0.1", 8900);

    private Config config() {
        return ConfigFactory.parseMap(HashMap.of(
            "node.path", testFolder.getRoot().getAbsolutePath(),
            "raft.enabled", true,
            "transport.port", 8900
        ).toJavaMap());
    }

    private FileService fileService() throws Exception {
        return new FileService(config(), streamService());
    }

    private StreamService streamService() throws Exception {
        return new InternalStreamService(Collections.emptySet());
    }

    private PersistentService persistent() throws Exception {
        return new FilePersistentService(
            fileService(),
            streamService()
        );
    }

    @Test
    public void testInitialState() throws Exception {
        PersistentService service = persistent();
        Assert.assertEquals(0, service.currentTerm());
        Assert.assertFalse(service.votedFor().isDefined());
    }

    @Test
    public void testUpdateState() throws Exception {
        PersistentService service = persistent();
        service.updateState(term1, Option.some(node));
        Assert.assertEquals(term1, service.currentTerm());
        Assert.assertEquals(Option.some(node), service.votedFor());
    }

    @Test
    public void testReopenState() throws Exception {
        persistent().updateState(term1, Option.some(node));
        PersistentService service = persistent();
        Assert.assertEquals(term1, service.currentTerm());
        Assert.assertEquals(Option.some(node), service.votedFor());
    }
}
