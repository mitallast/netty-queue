package org.mitallast.queue.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mitallast.queue.action.queue.dequeue.DeQueueRequest;
import org.mitallast.queue.action.queue.dequeue.DeQueueResponse;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.node.Node;

import java.nio.charset.Charset;
import java.util.UUID;

public abstract class BaseQueueTest {

    public final static Charset UTF8 = Charset.forName("UTF-8");
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    private Node node;
    private String queueName;
    private Settings settings;

    @Before
    public void setUp() throws Exception {
        settings = ImmutableSettings.builder()
                .put("work_dir", testFolder.newFolder().getAbsolutePath())
                .put("rest.transport.host", "127.0.0.1")
                .put("rest.transport.port", 18080)
                .put("stomp.transport.host", "127.0.0.1")
                .put("stomp.transport.port", 19080)
                .build();
        queueName = UUID.randomUUID().toString();
        node = new InternalNode(settings);
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

    public Node node() {
        return node;
    }

    public Client client() {
        return node.client();
    }

    public void createQueue() throws Exception {
        client().queues()
                .createQueue(new CreateQueueRequest(queueName(), ImmutableSettings.EMPTY))
                .get();
        assertQueueEmpty();
    }

    public DeQueueResponse dequeue() throws Exception {
        DeQueueRequest request = new DeQueueRequest();
        request.setQueue(queueName);
        return client().queue().dequeueRequest(request).get();
    }

    public void assertQueueEmpty() throws Exception {
        QueueStatsResponse response = client().queue()
                .queueStatsRequest(new QueueStatsRequest(queueName()))
                .get();
        assert response.getStats().getSize() == 0 : response.getStats();
    }
}
