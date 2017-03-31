package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.multibindings.Multibinder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.common.BaseClusterTest;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.resource.ResourceRegistry;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.mitallast.queue.raft.RaftState.Leader;

public class ClusterRaftTest extends BaseClusterTest {

    private ImmutableList<RegisterClient> client;
    private ImmutableList<RegisterByteClient> byteClient;

    protected AbstractModule[] testModules() {
        return new AbstractModule[]{
            new TestModule()
        };
    }

    @Before
    public void setUpNodes() throws Exception {
        super.setUpNodes();
        client = Immutable.map(nodes, n -> n.injector().getInstance(RegisterClient.class));
        byteClient = Immutable.map(nodes, n -> n.injector().getInstance(RegisterByteClient.class));
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
