package org.mitallast.queue.raft.persistent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Optional;

public class PersistentTest extends BaseTest {

    private final long term1 = 1;
    private final DiscoveryNode node = new DiscoveryNode("127.0.0.1", 8900);

    private Config config() {
        return ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
            .put("node.path", testFolder.getRoot().getAbsolutePath())
            .put("raft.enabled", true)
            .build());
    }

    private FileService fileService() throws Exception {
        return new FileService(config(), streamService());
    }

    private StreamService streamService() throws Exception {
        return new InternalStreamService(ImmutableSet.of());
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
        Assert.assertFalse(service.votedFor().isPresent());
    }

    @Test
    public void testUpdateState() throws Exception {
        PersistentService service = persistent();
        service.updateState(term1, Optional.of(node));
        Assert.assertEquals(term1, service.currentTerm());
        Assert.assertEquals(node, service.votedFor().orElseGet(null));
    }

    @Test
    public void testReopenState() throws Exception {
        persistent().updateState(term1, Optional.of(node));
        PersistentService service = persistent();
        Assert.assertEquals(term1, service.currentTerm());
        Assert.assertEquals(node, service.votedFor().orElseGet(null));
    }
}
