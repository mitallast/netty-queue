package org.mitallast.queue.raft;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.multibindings.Multibinder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import javaslang.collection.Vector;
import javaslang.concurrent.Future;
import javaslang.control.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseClusterTest;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.resource.ResourceRegistry;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Random;

import static org.mitallast.queue.raft.RaftState.Leader;

public class ClusterRaftTest extends BaseClusterTest {

    private Vector<Raft> raft;
    private Vector<RegisterClient> client;
    private Vector<RegisterByteClient> byteClient;

    protected AbstractModule[] testModules() {
        return new AbstractModule[]{
            new TestModule()
        };
    }

    @Before
    public void setUpNodes() throws Exception {
        createLeader();
        createFollower();
        createFollower();
        raft = nodes.map(n -> n.injector().getInstance(Raft.class));
        client = nodes.map(n -> n.injector().getInstance(RegisterClient.class));
        byteClient = nodes.map(n -> n.injector().getInstance(RegisterByteClient.class));
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
        for (int i = 0; i < nodes.size(); i++) {
            if (raft.get(i).currentState() == Leader) {
                leader = i;
            }
        }

        // warm up
        for (int i = 0; i < 10000; i++) {
            String value = client.get(leader).set("hello world " + i).get();
            Assert.assertEquals("hello world " + i, value);
        }

        for (int t = 0; t < 3; t++) {
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
    }

    @Test
    public void benchmarkAsynchronous() throws Exception {
        awaitElection();
        int leader = 0;
        for (int i = 0; i < nodes.size(); i++) {
            if (raft.get(i).currentState() == Leader) {
                leader = i;
            }
        }

        // warm up
        for (int i = 0; i < 10000; i++) {
            String value = client.get(leader).set("hello world " + i).get();
            Assert.assertEquals("hello world " + i, value);
        }

        for (int t = 0; t < 3; t++) {
            final int total = 200000;
            final ArrayList<Future<String>> futures = new ArrayList<>(total);
            final long start = System.currentTimeMillis();
            for (int i = 0; i < total; i++) {
                Future<String> future = client.get(leader).set("hello world " + i);
                futures.add(future);
            }
            for (int i = 0; i < total; i++) {
                Future<String> future = futures.get(i);
                String value = future.get();
                Assert.assertEquals("hello world " + i, value);
            }
            final long end = System.currentTimeMillis();
            printQps("raft command rpc", total, start, end);
        }
    }

    @Test
    public void benchmarkAsynchronousByte() throws Exception {
        awaitElection();
        int leader = 0;
        for (int i = 0; i < nodes.size(); i++) {
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

        for (int t = 0; t < 3; t++) {
            final int total = 200000;
            final ArrayList<Future<RegisterByteOK>> futures = new ArrayList<>(total);
            final long start = System.currentTimeMillis();
            for (int i = 0; i < total; i++) {
                Future<RegisterByteOK> future = byteClient.get(leader).set(Unpooled.wrappedBuffer(bytes));
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

        public Streamable handle(long index, RegisterSet registerSet) {
            logger.debug("prev value: {} new value: {}", value, registerSet.value);
            value = registerSet.value;
            return new RegisterValue(value);
        }

        public Streamable handle(long index, RegisterGet message) {
            return new RegisterValue(value);
        }

        public Streamable handle(long index, RegisterByteSet set) {
            logger.debug("prev value: {} new value: {}", value, set.buff);
            if (buff != null) {
                buff.release();
            }
            buff = set.buff;
            return new RegisterByteOK();
        }

        public Streamable handle(long index, RegisterByteGet message) {
            return new RegisterByteValue(buff);
        }

        @Override
        public Option<Streamable> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
            return Option.none();
        }
    }

    public static class RegisterSet implements Streamable {
        private final String value;

        public RegisterSet(StreamInput streamInput) {
            this.value = streamInput.readText();
        }

        public RegisterSet(String value) {
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput stream) {
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

        public RegisterGet(StreamInput streamInput) {
        }

        public RegisterGet() {
        }

        @Override
        public void writeTo(StreamOutput stream) {
        }

        @Override
        public String toString() {
            return "RegisterGet{}";
        }
    }

    public static class RegisterValue implements Streamable {
        private final String value;

        public RegisterValue(StreamInput streamInput) {
            this.value = streamInput.readText();
        }

        public RegisterValue(String value) {
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeText(value);
        }

        @Override
        public String toString() {
            return "RegisterValue{value=" + value + '}';
        }
    }

    public static class RegisterByteSet implements Streamable {
        private final ByteBuf buff;

        public RegisterByteSet(ByteBuf buff) {
            this.buff = buff;
        }

        public RegisterByteSet(StreamInput stream) {
            buff = stream.readByteBuf();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeByteBuf(buff);
        }
    }

    public static class RegisterByteOK implements Streamable {
        public RegisterByteOK() {
        }

        public RegisterByteOK(StreamInput stream) {
        }

        @Override
        public void writeTo(StreamOutput stream) {
        }
    }

    public static class RegisterByteGet implements Streamable {
        public RegisterByteGet() {
        }

        public RegisterByteGet(StreamInput stream) {
        }

        @Override
        public void writeTo(StreamOutput stream) {
        }
    }

    public static class RegisterByteValue implements Streamable {
        private final ByteBuf buff;

        public RegisterByteValue(ByteBuf buff) {
            this.buff = buff;
        }

        public RegisterByteValue(StreamInput stream) {
            buff = stream.readByteBuf();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeByteBuf(buff);
        }
    }

    public static class RegisterClient {
        private final Raft raft;

        @Inject
        public RegisterClient(Raft raft) {
            this.raft = raft;
        }

        public Future<String> set(String value) {
            return raft.command(new RegisterSet(value))
                .filter(m -> m instanceof RegisterValue)
                .map(m -> ((RegisterValue) m).value);
        }

        public Future<String> get() {
            return raft.command(new RegisterGet())
                .filter(m -> m instanceof RegisterValue)
                .map(m -> ((RegisterValue) m).value);
        }
    }

    public static class RegisterByteClient {
        private final Raft raft;

        @Inject
        public RegisterByteClient(Raft raft) {
            this.raft = raft;
        }

        public Future<RegisterByteOK> set(ByteBuf value) {
            return raft.command(new RegisterByteSet(value))
                .filter(m -> m instanceof RegisterByteOK)
                .map(m -> (RegisterByteOK) m);
        }

        public Future<ByteBuf> get() {
            return raft.command(new RegisterByteGet())
                .filter(m -> m instanceof RegisterByteValue)
                .map(m -> ((RegisterByteValue) m).buff);
        }
    }
}
