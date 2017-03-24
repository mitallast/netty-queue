package org.mitallast.queue.raft.persistent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.proto.ProtoService;
import org.mitallast.queue.proto.raft.DiscoveryNode;

import java.util.Optional;

public class PersistentTest extends BaseTest {

    private final long term1 = 1;
    private final DiscoveryNode node = DiscoveryNode.newBuilder()
        .setHost("127.0.0.1")
        .setPort(8900)
        .build();

    private Config config() {
        return ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
            .put("node.name", "test")
            .put("node.path", testFolder.getRoot().getAbsolutePath())
            .put("raft.enabled", true)
            .build());
    }

    private FileService fileService() throws Exception {
        return new FileService(config());
    }

    private ProtoService protoService() throws Exception {
        return new ProtoService(ImmutableSet.of());
    }

    private PersistentService persistent() throws Exception {
        return new FilePersistentService(
            config(),
            fileService(),
            protoService()
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
