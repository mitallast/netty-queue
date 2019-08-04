package org.mitallast.queue.common;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.util.ResourceLeakDetector;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.junit.After;
import org.junit.Before;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.node.InternalNode;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BaseIntegrationTest extends BaseTest {
    static {
        Codec.Companion.register(100, TestStreamable.class, TestStreamable.codec);
    }

    private final static AtomicInteger nodeCounter = new AtomicInteger(0);
    private List<InternalNode> nodes = new CopyOnWriteArrayList<>();

    protected InternalNode createNode() throws Exception {
        return createNode(config());
    }

    protected InternalNode createNode(Config config) throws Exception {
        InternalNode node = InternalNode.Companion.build(config);
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
                node.stop();
                node.close();
            }))
            .collect(Collectors.toList());
        for (Future<Void> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
    }

    protected Config config() throws Exception {
        int nodeId = nodeCounter.incrementAndGet();
        return ConfigFactory.parseMap(HashMap.ofEntries(
            Map.entry("nodes.name", "nodes" + nodeId),
            Map.entry("node.path", testFolder.newFolder().getAbsolutePath()),
            Map.entry("rest.transport.host", "127.0.0.1"),
            Map.entry("rest.transport.port", 18000 + random.nextInt(500)),
            Map.entry("transport.host", "127.0.0.1"),
            Map.entry("transport.port", 20000 + random.nextInt(500))
        ).toJavaMap()).withFallback(ConfigFactory.defaultReference());
    }

    public static class TestStreamable implements Message {
        public static final Codec<TestStreamable> codec = Codec.Companion.of(
            TestStreamable::new,
            TestStreamable::value,
            Codec.Companion.longCodec()
        );

        private final long value;

        public TestStreamable(long value) {
            this.value = value;
        }

        public long value() {
            return value;
        }

        @Override
        public String toString() {
            return "TestStreamable{" +
                "value=" + value +
                '}';
        }
    }
}
