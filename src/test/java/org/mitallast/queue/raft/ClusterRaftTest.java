package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.multibindings.Multibinder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.resource.ResourceRegistry;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.mitallast.queue.raft.RaftState.Follower;
import static org.mitallast.queue.raft.RaftState.Leader;

public class ClusterRaftTest extends BaseTest {
    private static final int nodesCount = 3;

    private ImmutableList<InternalNode> node;
    private ImmutableList<Raft> raft;
    private ImmutableList<RegisterClient> client;
    private ImmutableList<RegisterByteClient> byteClient;

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
                .put("transport.max_connections", 1)
                .build());
            bootstrap = false;
            builder.add(new InternalNode(config, new TestModule()));
        }
        node = builder.build();
        raft = ImmutableList.copyOf(node.stream().map(node -> node.injector().getInstance(Raft.class)).iterator());
        client = ImmutableList.copyOf(node.stream().map(node -> node.injector().getInstance(RegisterClient.class)).iterator());
        byteClient = ImmutableList.copyOf(node.stream().map(node -> node.injector().getInstance(RegisterByteClient.class)).iterator());

        ArrayList<Future> futures = new ArrayList<>();
        for (InternalNode item : this.node) {
            Future future = submit(() -> {
                try {
                    item.start();
                } catch (IOException e) {
                    assert false : e;
                }
            });
            futures.add(future);
        }
        for (Future future : futures) {
            future.get(10, TimeUnit.SECONDS);
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

        // warm up
        for (int i = 0; i < 10000; i++) {
            String value = client.get(leader).set("hello world " + i).get();
            Assert.assertEquals("hello world " + i, value);
        }

        // bench
        final long total = 10000;
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

        // warm up
        for (int i = 0; i < 10000; i++) {
            String value = client.get(leader).set("hello world " + i).get();
            Assert.assertEquals("hello world " + i, value);
        }

        final int total = 200000;
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

    @Test
    public void benchmarkAsynchronousByte() throws Exception {
        awaitElection();
        int leader = 0;
        for (int i = 0; i < nodesCount; i++) {
            if (raft.get(i).currentState() == Leader) {
                leader = i;
            }
        }

        byte[] bytes = new byte[4096];
        new Random().nextBytes(bytes);

        // warm up
        for (int i = 0; i < 10000; i++) {
            byteClient.get(leader).set(Unpooled.wrappedBuffer(bytes)).get();
        }

        final int total = 200000;
        final ArrayList<CompletableFuture<RegisterByteOK>> futures = new ArrayList<>(total);
        final long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            CompletableFuture<RegisterByteOK> future = byteClient.get(leader).set(Unpooled.wrappedBuffer(bytes));
            futures.add(future);
        }
        for (int i = 0; i < total; i++) {
            futures.get(i).get();
        }
        final long end = System.currentTimeMillis();


        BigInteger totalBytes = BigInteger.valueOf(bytes.length).multiply(BigInteger.valueOf(total));
        BigInteger bytesPerSec = totalBytes
            .multiply(BigInteger.valueOf(1000)) // to sec
            .divide(BigInteger.valueOf(end - start)); // duration in ms

        logger.info("messages    : {}", total);
        logger.info("message size: {}", bytes.length);
        logger.info("data size   : {}MB", totalBytes.divide(BigInteger.valueOf(1024 * 1024)));
        logger.info("total time  : {}ms", end - start);
        logger.info("throughput  : {}MB/s", bytesPerSec.divide(BigInteger.valueOf(1024 * 1024)));
    }

    private void awaitElection() throws Exception {
        while (true) {
            if (raft.stream().anyMatch(raft -> raft.currentState() == Leader)) {
                if (raft.stream().allMatch(raft -> raft.replicatedLog().committedIndex() == nodesCount)) {
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
            bind(RegisterByteClient.class).asEagerSingleton();
            bind(RegisterResourceFSM.class).asEagerSingleton();
            bind(ResourceFSM.class).to(RegisterResourceFSM.class);

            Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterSet.class, RegisterSet::new, 900000));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterGet.class, RegisterGet::new, 900001));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterValue.class, RegisterValue::new, 900002));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterByteSet.class, RegisterByteSet::new, 900010));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterByteOK.class, RegisterByteOK::new, 900011));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterByteGet.class, RegisterByteGet::new, 900012));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RegisterByteValue.class, RegisterByteValue::new, 900013));
        }
    }

    public static class RegisterResourceFSM implements ResourceFSM {
        private final Logger logger = LogManager.getLogger();
        private volatile String value = "";
        private volatile ByteBuf buff = Unpooled.EMPTY_BUFFER;

        @Inject
        public RegisterResourceFSM(ResourceRegistry registry) {
            registry.register(this);
            registry.register(RegisterSet.class, this::handle);
            registry.register(RegisterGet.class, this::handle);
            registry.register(RegisterByteSet.class, this::handle);
            registry.register(RegisterByteGet.class, this::handle);
        }

        public Streamable handle(RegisterSet registerSet) {
            logger.debug("prev value: {} new value: {}", value, registerSet.value);
            value = registerSet.value;
            return new RegisterValue(registerSet.requestId, value);
        }

        public Streamable handle(RegisterGet message) {
            return new RegisterValue(message.requestId, value);
        }

        public Streamable handle(RegisterByteSet set) {
            logger.debug("prev value: {} new value: {}", value, set.buff);
            buff = set.buff;
            return new RegisterByteOK(set.requestId);
        }

        public Streamable handle(RegisterByteGet message) {
            return new RegisterByteValue(message.requestId, buff);
        }

        @Override
        public Optional<Streamable> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
            return Optional.empty();
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

    public static class RegisterByteSet implements Streamable {
        private final long requestId;
        private final ByteBuf buff;

        public RegisterByteSet(long requestId, ByteBuf buff) {
            this.requestId = requestId;
            this.buff = buff;
        }

        public RegisterByteSet(StreamInput stream) throws IOException {
            requestId = stream.readLong();
            buff = stream.readByteBuf();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(requestId);
            stream.writeByteBuf(buff);
        }
    }

    public static class RegisterByteOK implements Streamable {
        private final long requestId;

        public RegisterByteOK(long requestId) {
            this.requestId = requestId;
        }

        public RegisterByteOK(StreamInput stream) throws IOException {
            requestId = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(requestId);
        }
    }

    public static class RegisterByteGet implements Streamable {
        private final long requestId;

        public RegisterByteGet(long requestId) {
            this.requestId = requestId;
        }

        public RegisterByteGet(StreamInput stream) throws IOException {
            requestId = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(requestId);
        }
    }

    public static class RegisterByteValue implements Streamable {
        private final long requestId;
        private final ByteBuf buff;

        public RegisterByteValue(long requestId, ByteBuf buff) {
            this.requestId = requestId;
            this.buff = buff;
        }

        public RegisterByteValue(StreamInput stream) throws IOException {
            requestId = stream.readLong();
            buff = stream.readByteBuf();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(requestId);
            stream.writeByteBuf(buff);
        }
    }

    public static class RegisterClient {
        private final Logger logger = LogManager.getLogger();
        private final Raft raft;
        private final ClusterDiscovery clusterDiscovery;
        private final AtomicLong counter = new AtomicLong();
        private final ConcurrentMap<Long, CompletableFuture<String>> requests = new ConcurrentHashMap<>();

        @Inject
        public RegisterClient(Raft raft, ClusterDiscovery clusterDiscovery, TransportController controller) {
            this.raft = raft;
            this.clusterDiscovery = clusterDiscovery;

            controller.registerMessageHandler(RegisterValue.class, this::receive);
        }

        public CompletableFuture<String> set(String value) {
            long request = counter.incrementAndGet();
            CompletableFuture<String> future = new CompletableFuture<>();
            requests.put(request, future);
            raft.apply(new ClientMessage(clusterDiscovery.self(), new RegisterSet(request, value)));
            return future;
        }

        public CompletableFuture<String> get() {
            long request = counter.incrementAndGet();
            CompletableFuture<String> future = new CompletableFuture<>();
            requests.put(request, future);
            raft.apply(new ClientMessage(clusterDiscovery.self(), new RegisterGet(request)));
            return future;
        }

        private void receive(RegisterValue event) {
            logger.debug("client received: {}", event);
            requests.get(event.requestId).complete(event.value);
        }
    }

    public static class RegisterByteClient {
        private final Logger logger = LogManager.getLogger();

        private final Raft raft;
        private final ClusterDiscovery clusterDiscovery;
        private final AtomicLong counter = new AtomicLong();
        private final ConcurrentMap<Long, CompletableFuture<RegisterByteOK>> setRequests = new ConcurrentHashMap<>();
        private final ConcurrentMap<Long, CompletableFuture<ByteBuf>> getRequests = new ConcurrentHashMap<>();

        @Inject
        public RegisterByteClient(Raft raft, ClusterDiscovery clusterDiscovery, TransportController controller) {
            this.raft = raft;
            this.clusterDiscovery = clusterDiscovery;

            controller.registerMessageHandler(RegisterByteOK.class, this::receiveOk);
            controller.registerMessageHandler(RegisterByteValue.class, this::receiveValue);
        }

        public CompletableFuture<RegisterByteOK> set(ByteBuf value) {
            long request = counter.incrementAndGet();
            CompletableFuture<RegisterByteOK> future = new CompletableFuture<>();
            setRequests.put(request, future);
            raft.apply(new ClientMessage(clusterDiscovery.self(), new RegisterByteSet(request, value)));
            return future;
        }

        public CompletableFuture<ByteBuf> get() {
            long request = counter.incrementAndGet();
            CompletableFuture<ByteBuf> future = new CompletableFuture<>();
            getRequests.put(request, future);
            raft.apply(new ClientMessage(clusterDiscovery.self(), new RegisterByteGet(request)));
            return future;
        }

        private void receiveOk(RegisterByteOK event) {
            logger.debug("client received: {}", event);
            setRequests.get(event.requestId).complete(event);
        }

        private void receiveValue(RegisterByteValue event) {
            logger.debug("client received: {}", event);
            getRequests.get(event.requestId).complete(event.buff);
        }
    }
}
