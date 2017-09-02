package org.mitallast.queue.crdt;

import com.google.inject.AbstractModule;
import javaslang.collection.Vector;
import javaslang.control.Option;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseClusterTest;
import org.mitallast.queue.common.ConfigBuilder;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.commutative.GCounter;
import org.mitallast.queue.crdt.commutative.GSet;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.commutative.OrderedGSet;
import org.mitallast.queue.crdt.routing.ResourceType;
import org.mitallast.queue.raft.ClusterRaftTest;

import java.io.IOException;


public class ClusterCrdtTest extends BaseClusterTest {
    static {
        Codec.Companion.register(99000000, TestLong.class, TestLong.codec);
    }

    private Vector<CrdtService> crdtServices;

    @Override
    protected ConfigBuilder config() throws IOException {
        return super.config()
            .with("crdt.replicas", 3)
            .with("crdt.buckets", 1);
    }

    @Before
    public void setUpNodes() throws Exception {
        createLeader();
        createFollower();
        createFollower();
        crdtServices = nodes.map(n -> n.injector().getInstance(CrdtService.class));
    }

    @Override
    protected AbstractModule[] testModules() {
        return new AbstractModule[]{
            new TestModule()
        };
    }

    @Test
    public void testLWWRegister() throws Exception {
        awaitElection();

        long total = 1000000;

        for (long c = 0; c < 10; c++) {
            final long crdt = c;
            createResource(crdt, ResourceType.LWWRegister);

            Vector<LWWRegister> registers = crdtServices
                .map(s -> s.bucket(crdt).registry())
                .map(r -> r.crdt(crdt, LWWRegister.class));

            long start = System.currentTimeMillis();
            for (long i = 0; i < total; i++) {
                registers.get((int) (i % nodes.size())).assign(new TestLong(i), i);
            }
            TestLong expected = new TestLong(total - 1);
            for (int w = 0; w < 1000; w++) {
                if (!registers.forAll(r -> r.value().contains(expected))) {
                    Thread.sleep(10);
                    continue;
                }
                break;
            }
            long end = System.currentTimeMillis();

            for (LWWRegister register : registers) {
                Assert.assertEquals(Option.some(expected), register.value());
            }

            printQps("CRDT lww-register", total, start, end);
        }
    }

    @Test
    public void testGCounter() throws Exception {
        awaitElection();

        long total = 1000000;

        for (long c = 0; c < 10; c++) {
            final long crdt = c;
            createResource(crdt, ResourceType.GCounter);

            Vector<GCounter> counters = crdtServices
                .map(s -> s.bucket(crdt).registry())
                .map(r -> r.crdt(crdt, GCounter.class));

            long start = System.currentTimeMillis();
            executeConcurrent((thread, concurrency) -> {
                for (long i = thread; i < total; i += concurrency) {
                    counters.get((int) (i % nodes.size())).increment();
                }
            });
            for (int w = 0; w < 100; w++) {
                if (!counters.forAll(r -> r.value() == total)) {
                    Thread.sleep(10);
                    continue;
                }
                break;
            }
            long end = System.currentTimeMillis();

            for (GCounter counter : counters) {
                Assert.assertEquals(total, counter.value());
            }

            printQps("CRDT g-counter", total, start, end);
        }
    }

    @Test
    public void testGSet() throws Exception {
        awaitElection();

        long total = 1000000;

        for (long c = 0; c < 3; c++) {
            final long crdt = c;
            createResource(crdt, ResourceType.GSet);

            Vector<GSet> sets = crdtServices
                .map(s -> s.bucket(crdt).registry())
                .map(r -> r.crdt(crdt, GSet.class));

            long start = System.currentTimeMillis();
            executeConcurrent((thread, concurrency) -> {
                for (long i = thread; i < total; i += concurrency) {
                    sets.get((int) (i % nodes.size())).add(new TestLong(i));
                }
            });

            for (int w = 0; w < 1000; w++) {
                if (!sets.forAll(r -> r.values().length() == total)) {
                    Thread.sleep(10);
                    continue;
                }
                break;
            }
            long end = System.currentTimeMillis();

            for (GSet set : sets) {
                Assert.assertEquals(total, set.values().length());
            }

            printQps("CRDT g-set", total, start, end);
        }
    }

    @Test
    public void testOrderedGSet() throws Exception {
        awaitElection();

        long total = 1000000;

        for (long c = 0; c < 3; c++) {
            final long crdt = c;
            createResource(crdt, ResourceType.OrderedGSet);

            Vector<OrderedGSet> sets = crdtServices
                .map(s -> s.bucket(crdt).registry())
                .map(r -> r.crdt(crdt, OrderedGSet.class));

            long start = System.currentTimeMillis();
            executeConcurrent((thread, concurrency) -> {
                for (long i = thread; i < total; i += concurrency) {
                    sets.get((int) (i % nodes.size())).add(new TestLong(i), i);
                }
            });

            for (int w = 0; w < 1000; w++) {
                if (!sets.forAll(r -> r.values().length() == total)) {
                    Thread.sleep(10);
                    continue;
                }
                break;
            }
            long end = System.currentTimeMillis();

            for (OrderedGSet set : sets) {
                Assert.assertEquals(total, set.values().length());
            }

            printQps("CRDT ordered-g-set", total, start, end);
        }
    }

    private void createResource(long crdt, ResourceType type) throws Exception {
        crdtServices.head().addResource(crdt, type).get();
        for (int w = 0; w < 10; w++) {
            if (!crdtServices
                .map(s -> s.bucket(crdt).registry())
                .forAll(r -> r.crdtOpt(crdt).isDefined())) {
                logger.info("await crdt {}", crdt);
                Thread.sleep(1000);
            }
        }
        assert crdtServices
            .map(s -> s.bucket(crdt).registry())
            .forAll(r -> r.crdtOpt(crdt).isDefined());
    }

    public static class TestModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(ClusterRaftTest.RegisterClient.class).asEagerSingleton();
        }
    }

    public static class TestLong implements Message {
        public static final Codec<TestLong> codec = Codec.Companion.of(
            TestLong::new,
            TestLong::value,
            Codec.Companion.longCodec()
        );

        private final long value;

        public TestLong(long value) {
            this.value = value;
        }

        public long value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestLong testLong = (TestLong) o;

            return value == testLong.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
}
