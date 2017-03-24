package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.multibindings.Multibinder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.proto.ProtoRegistry;
import org.mitallast.queue.common.proto.ProtoService;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.proto.raft.ClientMessage;
import org.mitallast.queue.proto.raft.RaftSnapshotMetadata;
import org.mitallast.queue.proto.test.*;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
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
        ByteString value = ByteString.copyFrom(bytes);

        // warm up
        for (int i = 0; i < 10000; i++) {
            byteClient.get(leader).set(value).get();
        }

        final int total = 200000;
        final ArrayList<CompletableFuture<RegisterByteOK>> futures = new ArrayList<>(total);
        final long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            CompletableFuture<RegisterByteOK> future = byteClient.get(leader).set(value);
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

            Multibinder<ProtoRegistry> protoBinder = Multibinder.newSetBinder(binder(), ProtoRegistry.class);

            protoBinder.addBinding().toInstance(new ProtoRegistry(2001, RegisterSet.getDescriptor(), RegisterSet.parser()));
            protoBinder.addBinding().toInstance(new ProtoRegistry(2002, RegisterGet.getDescriptor(), RegisterGet.parser()));
            protoBinder.addBinding().toInstance(new ProtoRegistry(2003, RegisterValue.getDescriptor(), RegisterValue.parser()));

            protoBinder.addBinding().toInstance(new ProtoRegistry(3001, RegisterByteSet.getDescriptor(), RegisterByteSet.parser()));
            protoBinder.addBinding().toInstance(new ProtoRegistry(3002, RegisterByteOK.getDescriptor(), RegisterByteOK.parser()));
            protoBinder.addBinding().toInstance(new ProtoRegistry(3003, RegisterByteGet.getDescriptor(), RegisterByteGet.parser()));
            protoBinder.addBinding().toInstance(new ProtoRegistry(3004, RegisterByteValue.getDescriptor(), RegisterByteValue.parser()));
        }
    }

    public static class RegisterResourceFSM extends AbstractComponent implements ResourceFSM {

        private volatile String value = "";
        private volatile ByteString buff = ByteString.EMPTY;

        @Inject
        public RegisterResourceFSM(Config config, ResourceRegistry registry) {
            super(config, RegisterResourceFSM.class);
            registry.register(this);
            registry.register(RegisterSet.class, this::handle);
            registry.register(RegisterGet.class, this::handle);
            registry.register(RegisterByteSet.class, this::handle);
            registry.register(RegisterByteGet.class, this::handle);
        }

        public Message handle(RegisterSet registerSet) {
            logger.debug("prev value: {} new value: {}", value, registerSet.getValue());
            value = registerSet.getValue();
            return RegisterValue.newBuilder()
                .setRequestId(registerSet.getRequestId())
                .setValue(value)
                .build();
        }

        public Message handle(RegisterGet message) {
            return RegisterValue.newBuilder()
                .setRequestId(message.getRequestId())
                .setValue(value)
                .build();
        }

        public Message handle(RegisterByteSet set) {
            logger.debug("prev value: {} new value: {}", value, set.getValue());
            buff = set.getValue();
            return RegisterByteOK.newBuilder().setRequestId(set.getRequestId()).build();
        }

        public Message handle(RegisterByteGet message) {
            return RegisterByteValue.newBuilder().setRequestId(message.getRequestId()).setValue(buff).build();
        }

        @Override
        public Optional<Message> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
            return Optional.empty();
        }
    }

    public static class RegisterClient extends AbstractComponent {
        private final Raft raft;
        private final ClusterDiscovery clusterDiscovery;
        private final ProtoService protoService;
        private final AtomicLong counter = new AtomicLong();
        private final ConcurrentMap<Long, CompletableFuture<String>> requests = new ConcurrentHashMap<>();

        @Inject
        public RegisterClient(Config config, Raft raft, ClusterDiscovery clusterDiscovery, TransportController controller, ProtoService protoService) {
            super(config, RegisterClient.class);
            this.raft = raft;
            this.clusterDiscovery = clusterDiscovery;
            this.protoService = protoService;

            controller.registerMessageHandler(RegisterValue.class, this::receive);
        }

        public CompletableFuture<String> set(String value) {
            long request = counter.incrementAndGet();
            CompletableFuture<String> future = new CompletableFuture<>();
            requests.put(request, future);
            raft.apply(ClientMessage.newBuilder()
                .setClient(clusterDiscovery.self())
                .setCommand(protoService.pack(RegisterSet.newBuilder().setRequestId(request).setValue(value).build()))
                .build());
            return future;
        }

        public CompletableFuture<String> get() {
            long request = counter.incrementAndGet();
            CompletableFuture<String> future = new CompletableFuture<>();
            requests.put(request, future);
            raft.apply(ClientMessage.newBuilder()
                .setClient(clusterDiscovery.self())
                .setCommand(protoService.pack(RegisterGet.newBuilder().setRequestId(request).build()))
                .build()
            );
            return future;
        }

        private void receive(RegisterValue event) {
            logger.debug("client received: {}", event);
            requests.get(event.getRequestId()).complete(event.getValue());
        }
    }

    public static class RegisterByteClient extends AbstractComponent {
        private final Raft raft;
        private final ClusterDiscovery clusterDiscovery;
        private final ProtoService protoService;
        private final AtomicLong counter = new AtomicLong();
        private final ConcurrentMap<Long, CompletableFuture<RegisterByteOK>> setRequests = new ConcurrentHashMap<>();
        private final ConcurrentMap<Long, CompletableFuture<ByteString>> getRequests = new ConcurrentHashMap<>();

        @Inject
        public RegisterByteClient(Config config, Raft raft, ClusterDiscovery clusterDiscovery, TransportController controller, ProtoService protoService) {
            super(config, RegisterByteClient.class);
            this.raft = raft;
            this.clusterDiscovery = clusterDiscovery;
            this.protoService = protoService;

            controller.registerMessageHandler(RegisterByteOK.class, this::receiveOk);
            controller.registerMessageHandler(RegisterByteValue.class, this::receiveValue);
        }

        public CompletableFuture<RegisterByteOK> set(ByteString value) {
            long request = counter.incrementAndGet();
            CompletableFuture<RegisterByteOK> future = new CompletableFuture<>();
            setRequests.put(request, future);
            raft.apply(ClientMessage.newBuilder()
                .setClient(clusterDiscovery.self())
                .setCommand(protoService.pack(RegisterByteSet.newBuilder().setRequestId(request).setValue(value).build()))
                .build()
            );
            return future;
        }

        public CompletableFuture<ByteString> get() {
            long request = counter.incrementAndGet();
            CompletableFuture<ByteString> future = new CompletableFuture<>();
            getRequests.put(request, future);
            raft.apply(ClientMessage.newBuilder()
                .setClient(clusterDiscovery.self())
                .setCommand(protoService.pack(RegisterByteGet.newBuilder().setRequestId(request).build()))
                .build()
            );
            return future;
        }

        private void receiveOk(RegisterByteOK event) {
            logger.debug("client received: {}", event);
            setRequests.get(event.getRequestId()).complete(event);
        }

        private void receiveValue(RegisterByteValue event) {
            logger.debug("client received: {}", event);
            getRequests.get(event.getRequestId()).complete(event.getValue());
        }
    }
}
