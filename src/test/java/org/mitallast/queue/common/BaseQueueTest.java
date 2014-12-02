package org.mitallast.queue.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.node.Node;

public abstract class BaseQueueTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private Node node;

    @Before
    public void setUp() throws Exception {
        node = new InternalNode(ImmutableSettings.builder()
            .put("work_dir", testFolder.newFolder().getAbsolutePath())
            .build());
        node.start();
    }

    @After
    public void tearDown() throws Exception {
        node.stop();
        node.close();
        node = null;
    }

    public Node node() {
        return node;
    }

    public Client client() {
        return node.client();
    }
}
