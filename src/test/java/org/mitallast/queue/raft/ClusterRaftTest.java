package org.mitallast.queue.raft;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
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
import org.mitallast.queue.common.ConfigBuilder;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.resource.ResourceRegistry;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Random;

import static org.mitallast.queue.raft.RaftState.Leader;

public class ClusterRaftTest extends BaseClusterTest {

    static {
        Codec.Companion.register(900000, RegisterSet.class, RegisterSet.codec);
        Codec.Companion.register(900001, RegisterGet.class, RegisterGet.codec);
        Codec.Companion.register(900002, RegisterValue.class, RegisterValue.codec);

        Codec.Companion.register(900100, RegisterByteSet.class, RegisterByteSet.codec);
        Codec.Companion.register(900101, RegisterByteOK.class, RegisterByteOK.codec);
        Codec.Companion.register(900102, RegisterByteGet.class, RegisterByteGet.codec);
        Codec.Companion.register(900103, RegisterByteValue.class, RegisterByteValue.codec);
    }

    private Vector<Raft> raft;
    private Vector<RegisterClient> client;
    private Vector<RegisterByteClient> byteClient;

    protected AbstractModule[] testModules() {
        return new AbstractModule[]{
            new TestModule()
        };
    }

    @Override
    protected ConfigBuilder config() throws IOException {
        return super.config().with("crdt.enabled", false);
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

        public Message handle(long index, RegisterSet registerSet) {
            logger.debug("prev value: {} new value: {}", value, registerSet.value);
            value = registerSet.value;
            return new RegisterValue(value);
        }

        public Message handle(long index, RegisterGet message) {
            return new RegisterValue(value);
        }

        public Message handle(long index, RegisterByteSet set) {
            logger.debug("prev value: {} new value: {}", value, set.buf);
            if (buff != null) {
                buff.release();
            }
            buff = set.buf;
            return new RegisterByteOK();
        }

        public Message handle(long index, RegisterByteGet message) {
            return new RegisterByteValue(buff);
        }

        @Override
        public Option<Message> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
            return Option.none();
        }
    }

    public static class RegisterSet implements Message {
        public static final Codec<RegisterSet> codec = Codec.Companion.of(
            RegisterSet::new,
            RegisterSet::value,
            Codec.Companion.stringCodec()
        );

        private final String value;

        public RegisterSet(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        @Override
        public String toString() {
            return "RegisterSet{" +
                "value='" + value + '\'' +
                '}';
        }
    }

    public static class RegisterGet implements Message {
        public static final Codec<RegisterGet> codec = Codec.Companion.of(new RegisterGet());

        public RegisterGet() {
        }
    }

    public static class RegisterValue implements Message {
        public static final Codec<RegisterValue> codec = Codec.Companion.of(
            RegisterValue::new,
            RegisterValue::value,
            Codec.Companion.stringCodec()
        );

        private final String value;

        public RegisterValue(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        @Override
        public String toString() {
            return "RegisterValue{value=" + value + '}';
        }
    }

    public static class RegisterByteSet implements Message {
        public static final Codec<RegisterByteSet> codec = Codec.Companion.of(
            RegisterByteSet::new,
            RegisterByteSet::buf,
            Codec.Companion.byteBufCodec()
        );

        private final ByteBuf buf;

        public RegisterByteSet(ByteBuf buf) {
            this.buf = buf;
        }

        public ByteBuf buf() {
            return buf;
        }
    }

    public static class RegisterByteOK implements Message {
        public static final Codec<RegisterByteOK> codec = Codec.Companion.of(new RegisterByteOK());

        public RegisterByteOK() {
        }
    }

    public static class RegisterByteGet implements Message {
        public static final Codec<RegisterByteGet> codec = Codec.Companion.of(new RegisterByteGet());

        public RegisterByteGet() {
        }
    }

    public static class RegisterByteValue implements Message {
        public static final Codec<RegisterByteValue> codec = Codec.Companion.of(
            RegisterByteValue::new,
            RegisterByteValue::buf,
            Codec.Companion.byteBufCodec()
        );

        private final ByteBuf buf;

        public RegisterByteValue(ByteBuf buf) {
            this.buf = buf;
        }

        public ByteBuf buf() {
            return buf;
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
                .map(m -> ((RegisterByteValue) m).buf());
        }
    }
}
