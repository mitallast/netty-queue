package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.multibindings.Multibinder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.mitallast.queue.raft.RaftState.Follower;
import static org.mitallast.queue.raft.RaftState.Leader;

public class ClusterRaftTest extends BaseTest {
    private static final int nodesCount = 5;

    private ImmutableList<InternalNode> node;
    private ImmutableList<Raft> raft;
    private ImmutableList<RegisterClient> client;

    @Before
    public void setUpNodes() throws Exception {
        HashSet<Integer> ports = new HashSet<>();
        while (ports.size() < nodesCount) {
            ports.add(8800 + random.nextInt(99));
        }

        String nodeDiscovery = ports.stream().map(port -> "127.0.0.1:" + port).reduce((a, b) -> a + "," + b).orElse("");

        ImmutableList.Builder<InternalNode> builder = ImmutableList.builder();
        boolean bootstrap = true;
        for (Integer port : ports) {
            Config config = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
                .put("node.name", "node" + port)
                .put("node.path", testFolder.newFolder().getAbsolutePath())
                .put("rest.enabled", false)
                .put("blob.enabled", false)
                .put("raft.discovery.host", "127.0.0.1")
                .put("raft.discovery.port", port)
                .put("raft.discovery.nodes.0", nodeDiscovery)
                .put("raft.keep-init-until-found", nodesCount)
                .put("raft.election-deadline", "1s")
                .put("raft.heartbeat", "500ms")
                .put("raft.bootstrap", bootstrap)
                .put("raft.snapshot-interval", 10000)
                .put("transport.host", "127.0.0.1")
                .put("transport.port", port)
                .build());
            bootstrap = false;
            builder.add(new InternalNode(config, new TestModule()));
        }
        node = builder.build();
        raft = ImmutableList.copyOf(node.stream().map(node -> node.injector().getInstance(Raft.class)).iterator());
        client = ImmutableList.copyOf(node.stream().map(node -> node.injector().getInstance(RegisterClient.class)).iterator());

        for (InternalNode node : this.node) {
            node.start();
        }
    }

    @After
    public void tearDownNodes() throws Exception {
        for (InternalNode node : this.node) {
            node.stop();
        }
        for (InternalNode node : this.node) {
            node.close();
        }
    }

    @Test
    public void testLeaderElected() throws Exception {
        awaitElection();
        assertLeaderElected();
    }

    @Test
    public void testClientMessage() throws Exception {
        awaitElection();
        String value1 = client.get(1).get().get();
        Assert.assertEquals("", value1);

        String value2 = client.get(0).set("hello world").get();
        Assert.assertEquals("hello world", value2);
    }

    @Test
    public void benchmarkSynchronous() throws Exception {
        awaitElection();
        int leader = 0;
        for (int i = 0; i < nodesCount; i++) {
            if (raft.get(i).currentState() == Leader) {
                leader = i;
            }
        }

        final long total = 1000;
        final long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            String value = client.get(leader).set("hello world " + i).get();
            Assert.assertEquals("hello world " + i, value);
        }
        final long end = System.currentTimeMillis();
        printQps("raft command rpc", total, start, end);
    }

    @Test
    public void benchmarkAsynchronous() throws Exception {
        awaitElection();
        int leader = 0;
        for (int i = 0; i < nodesCount; i++) {
            if (raft.get(i).currentState() == Leader) {
                leader = i;
            }
        }

        final int total = 10000;
        final ArrayList<CompletableFuture<String>> futures = new ArrayList<>(total);
        final long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            CompletableFuture<String> future = client.get(leader).set("hello world " + i);
            futures.add(future);
        }
        for (int i = 0; i < total; i++) {
            CompletableFuture<String> future = futures.get(i);
            String value = future.get();
            Assert.assertEquals("hello world " + i, value);
        }

        final long end = System.currentTimeMillis();
        printQps("raft command rpc", total, start, end);
    }

    private void awaitElection() throws Exception {
        while (true) {
            if (raft.stream().anyMatch(raft -> raft.currentState() == Leader)) {
                if (raft.stream().allMatch(raft -> raft.currentLog().committedIndex() == nodesCount)) {
                    logger.info("leader found, cluster available");
                    return;
                }
            }
            Thread.sleep(10);
        }
    }

    private void assertLeaderElected() throws Exception {
        Assert.assertEquals(1, raft.stream().filter(raft -> raft.currentState() == Leader).count());
        Assert.assertEquals(nodesCount - 1, raft.stream().filter(raft -> raft.currentState() == Follower).count());
    }

    public static class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(RegisterClient.class).asEagerSingleton();
            bind(RegisterResourceFSM.class).asEagerSingleton();
            bind(ResourceFSM.class).to(RegisterResourceFSM.class);

            Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterSet.class, RegisterSet::new, 900000));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterGet.class, RegisterGet::new, 900001));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterValue.class, RegisterValue::new, 900002));
        }
    }

    public static class RegisterResourceFSM extends AbstractComponent implements ResourceFSM {

        private volatile String value = "";

        @Inject
        public RegisterResourceFSM(Config config) {
            super(config, RegisterResourceFSM.class);
        }

        @Override
        public Streamable apply(Streamable message) {
            if (message instanceof RegisterSet) {
                RegisterSet registerSet = (RegisterSet) message;
                logger.debug("prev value: {} new value: {}", value, registerSet.value);
                value = registerSet.value;
                return new RegisterValue(registerSet.requestId, value);
            } else if (message instanceof RegisterGet) {
                return new RegisterValue(((RegisterGet) message).requestId, value);
            }
            return null;
        }

        @Override
        public Optional<RaftSnapshot> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
            return Optional.of(new RaftSnapshot(snapshotMeta, new RegisterSet(-1, value)));
        }
    }

    public static class RegisterSet implements Streamable {
        private final long requestId;
        private final String value;

        public RegisterSet(StreamInput streamInput) throws IOException {
            this.requestId = streamInput.readLong();
            this.value = streamInput.readText();
        }

        public RegisterSet(long requestId, String value) {
            this.requestId = requestId;
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(requestId);
            stream.writeText(value);
        }

        @Override
        public String toString() {
            return "RegisterSet{" +
                "value='" + value + '\'' +
                '}';
        }
    }

    public static class RegisterGet implements Streamable {
        private final long requestId;

        public RegisterGet(StreamInput streamInput) throws IOException {
            requestId = streamInput.readLong();
        }

        public RegisterGet(long requestId) {
            this.requestId = requestId;
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(requestId);
        }

        @Override
        public String toString() {
            return "RegisterGet{" +
                "requestId=" + requestId +
                '}';
        }
    }

    public static class RegisterValue implements Streamable {

        private final long requestId;
        private final String value;

        public RegisterValue(StreamInput streamInput) throws IOException {
            this.requestId = streamInput.readLong();
            this.value = streamInput.readText();
        }

        public RegisterValue(long requestId, String value) {
            this.requestId = requestId;
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(requestId);
            stream.writeText(value);
        }

        @Override
        public String toString() {
            return "RegisterValue{" +
                "requestId=" + requestId +
                ", value='" + value + '\'' +
                '}';
        }
    }

    public static class RegisterClient extends AbstractComponent {

        private final Raft raft;
        private final ClusterDiscovery clusterDiscovery;
        private final AtomicLong counter = new AtomicLong();
        private final ConcurrentMap<Long, CompletableFuture<String>> requests = new ConcurrentHashMap<>();

        @Inject
        public RegisterClient(Config config, Raft raft, ClusterDiscovery clusterDiscovery, TransportController controller) {
            super(config, RegisterClient.class);
            this.raft = raft;
            this.clusterDiscovery = clusterDiscovery;

            controller.registerMessageHandler(RegisterSet.class, raft::receive);
            controller.registerMessageHandler(RegisterGet.class, raft::receive);
            controller.registerMessageHandler(RegisterValue.class, this::receive);
        }

        public CompletableFuture<String> set(String value) {
            long request = counter.incrementAndGet();
            CompletableFuture<String> future = new CompletableFuture<>();
            requests.put(request, future);
            raft.receive(new ClientMessage(clusterDiscovery.self(), new RegisterSet(request, value)));
            return future;
        }

        public CompletableFuture<String> get() {
            long request = counter.incrementAndGet();
            CompletableFuture<String> future = new CompletableFuture<>();
            requests.put(request, future);
            raft.receive(new ClientMessage(clusterDiscovery.self(), new RegisterGet(request)));
            return future;
        }

        private void receive(RegisterValue event) {
            logger.debug("client received: {}", event);
            requests.get(event.requestId).complete(event.value);
        }
    }
}
