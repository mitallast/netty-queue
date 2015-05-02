package org.mitallast.queue.common;

import com.google.common.collect.ImmutableMap;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Before;
import org.mitallast.queue.Version;
import org.mitallast.queue.action.queue.pop.PopRequest;
import org.mitallast.queue.action.queue.pop.PopResponse;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.cluster.DiscoveryNode;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.node.Node;

public abstract class BaseQueueTest extends BaseTest {

    private DiscoveryNode discoveryNode;
    private Node node;
    private String queueName;
    private Settings settings;

    @Before
    public void setUp() throws Exception {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        settings = ImmutableSettings.builder()
            .put("work_dir", testFolder.newFolder().getAbsolutePath())
            .put("rest.transport.host", "127.0.0.1")
            .put("rest.transport.port", 18000 + random.nextInt(500))
            .put("transport.host", "127.0.0.1")
            .put("transport.port", 20000 + random.nextInt(500))
            .build();

        discoveryNode = new DiscoveryNode(
            randomUUID(),
            settings.get("transport.host"),
            settings.getAsInt("transport.port", 20000),
            ImmutableMap.of(),
            Version.CURRENT
        );

        queueName = randomUUID().toString();
        node = new InternalNode(settings());
        node.start();
    }

    @After
    public void tearDown() throws Exception {
        node.stop();
        node.close();
        node = null;
    }

    public String queueName() {
        return queueName;
    }

    public Settings settings() {
        return settings;
    }

    public DiscoveryNode discoveryNode() {
        return discoveryNode;
    }

    public Node node() {
        return node;
    }

    public Client localClient() {
        return node.localClient();
    }

    public void createQueue() throws Exception {
        localClient().queues()
            .createQueue(new CreateQueueRequest(queueName(), ImmutableSettings.EMPTY))
            .get();
        assertQueueEmpty();
    }

    public PopResponse pop() throws Exception {
        PopRequest request = new PopRequest();
        request.setQueue(queueName);
        return localClient().queue().popRequest(request).get();
    }

    public void assertQueueEmpty() throws Exception {
        QueueStatsResponse response = localClient().queue()
            .queueStatsRequest(new QueueStatsRequest(queueName()))
            .get();
        assert response.getStats().getSize() == 0 : response.getStats();
    }
}
