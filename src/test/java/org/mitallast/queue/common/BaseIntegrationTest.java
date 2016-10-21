package org.mitallast.queue.common;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Before;
import org.mitallast.queue.node.InternalNode;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BaseIntegrationTest extends BaseTest {

    private final static AtomicInteger nodeCounter = new AtomicInteger(0);
    private List<InternalNode> nodes = new CopyOnWriteArrayList<>();

    protected InternalNode createNode() throws Exception {
        return createNode(config());
    }

    protected InternalNode createNode(Config config) throws Exception {
        InternalNode node = new InternalNode(config);
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
        List<Future<Void>> futures = nodes.stream()
            .map(node -> submit(() -> {
                try {
                    node.stop();
                    node.close();
                } catch (IOException e) {
                    assert false : e;
                }
            }))
            .collect(Collectors.toList());
        for (Future<Void> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
    }

    protected Config config() throws Exception {
        int nodeId = nodeCounter.incrementAndGet();
        ImmutableMap<String, Object> config = ImmutableMap.<String, Object>builder()
                .put("node.name", "node" + nodeId)
                .put("work_dir", testFolder.newFolder().getAbsolutePath())
                .put("rest.transport.host", "127.0.0.1")
                .put("rest.transport.port", 18000 + random.nextInt(500))
                .put("transport.host", "127.0.0.1")
                .put("transport.port", 20000 + random.nextInt(500))
                .build();
        return ConfigFactory.parseMap(config).withFallback(ConfigFactory.defaultReference());
    }
}
