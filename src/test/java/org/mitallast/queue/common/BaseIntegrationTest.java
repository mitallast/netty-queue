package org.mitallast.queue.common;

import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Before;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.node.InternalNode;

import java.util.ArrayList;
import java.util.List;

public class BaseIntegrationTest extends BaseTest {

    private List<InternalNode> nodes = new ArrayList<>();

    protected InternalNode createNode() throws Exception {
        InternalNode node = new InternalNode(settings());
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
        nodes.forEach(InternalNode::stop);
        nodes.forEach(InternalNode::close);
    }

    private Settings settings() throws Exception {
        return ImmutableSettings.builder()
            .put("work_dir", testFolder.newFolder().getAbsolutePath())
            .put("rest.transport.host", "127.0.0.1")
            .put("rest.transport.port", 18000 + random.nextInt(500))
            .put("transport.host", "127.0.0.1")
            .put("transport.port", 20000 + random.nextInt(500))
            .build();
    }
}
