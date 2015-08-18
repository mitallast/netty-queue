package org.mitallast.queue.common;

import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Before;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.node.InternalNode;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class BaseIntegrationTest extends BaseTest {

    private static int nodeCounter = 0;
    private List<InternalNode> nodes = new CopyOnWriteArrayList<>();

    protected InternalNode createNode() throws Exception {
        return createNode(settings());
    }

    protected InternalNode createNode(Settings settings) throws Exception {
        InternalNode node = new InternalNode(settings);
        node.start();
        nodes.add(node);
        return node;
    }

    @Before
    public void setUpResource() throws Exception {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
    }

    @After
    public void tearDownNodes() throws Exception {
        for (InternalNode node : nodes) {
            if (node.lifecycle().started()) {
                node.stop();
            }
        }
        for (InternalNode node : nodes) {
            if (node.lifecycle().stopped()) {
                node.close();
            }
        }
    }

    protected Settings settings() throws Exception {
        nodeCounter++;
        return ImmutableSettings.builder()
            .put("node.name", "node" + nodeCounter)
            .put("work_dir", testFolder.newFolder().getAbsolutePath())
            .put("rest.transport.host", "127.0.0.1")
            .put("rest.transport.port", 18000 + random.nextInt(500))
            .put("transport.host", "127.0.0.1")
            .put("transport.port", 20000 + random.nextInt(500))
            .build();
    }
}
