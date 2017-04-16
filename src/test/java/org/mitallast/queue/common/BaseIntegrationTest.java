package org.mitallast.queue.common;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.util.ResourceLeakDetector;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.junit.After;
import org.junit.Before;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.node.InternalNode;

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
        InternalNode node = new InternalNode(config, new TestModule());
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
            Map.entry("work_dir", testFolder.newFolder().getAbsolutePath()),
            Map.entry("rest.transport.host", "127.0.0.1"),
            Map.entry("rest.transport.port", 18000 + random.nextInt(500)),
            Map.entry("transport.host", "127.0.0.1"),
            Map.entry("transport.port", 20000 + random.nextInt(500))
        ).toJavaMap()).withFallback(ConfigFactory.defaultReference());
    }

    public class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(TestStreamable.class, TestStreamable::new, 100));
        }
    }

    public static class TestStreamable implements Streamable {

        private final long value;

        public TestStreamable(StreamInput streamInput) {
            this.value = streamInput.readLong();
        }

        public TestStreamable(long value) {
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeLong(value);
        }

        @Override
        public String toString() {
            return "TestStreamable{" +
                "value=" + value +
                '}';
        }
    }
}
