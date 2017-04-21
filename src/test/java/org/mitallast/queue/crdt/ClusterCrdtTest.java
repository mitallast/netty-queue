package org.mitallast.queue.crdt;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import javaslang.collection.Vector;
import javaslang.control.Option;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseClusterTest;
import org.mitallast.queue.common.ConfigBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.routing.ResourceType;
import org.mitallast.queue.crdt.routing.fsm.AddResource;
import org.mitallast.queue.raft.ClusterRaftTest;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

import static org.mitallast.queue.common.stream.StreamableRegistry.of;

public class ClusterCrdtTest extends BaseClusterTest {

    private Vector<Raft> raft;
    private Vector<CrdtService> crdtServices;
    private Vector<DiscoveryNode> discoveryNodes;

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
        raft = nodes.map(n -> n.injector().getInstance(Raft.class));
        crdtServices = nodes.map(n -> n.injector().getInstance(CrdtService.class));
        discoveryNodes = nodes.map(n -> n.injector().getInstance(ClusterDiscovery.class).self());
    }

    @Override
    protected AbstractModule[] testModules() {
        return new AbstractModule[]{
            new TestModule()
        };
    }

    @Test
    public void testReplicate() throws Exception {
        awaitElection();

        long total = 1000000;

        for (long c = 0; c < 1000000; c++) {
            final long crdt = c;
            raft.get(0).apply(new ClientMessage(discoveryNodes.get(0), new AddResource(crdt, ResourceType.LWWRegister)));
            awaitResourceAllocate(crdt);

            Vector<LWWRegister> registers = crdtServices
                .map(s -> s.bucket(crdt).registry())
                .map(r -> r.crdt(crdt, LWWRegister.class));

            long start = System.currentTimeMillis();
            for (long i = 0; i < total; i++) {
                registers.get((int) (i % nodes.size())).assign(new TestLong(i), i);
            }
            for (int w = 0; w < 10000; w++) {
                if (!registers.forAll(r -> r.value().contains(new TestLong(total - 1)))) {
                    Thread.sleep(10);
                    continue;
                }
                break;
            }
            long end = System.currentTimeMillis();

            for (CrdtService crdtService : crdtServices) {
                LWWRegister register = crdtService.bucket(crdt)
                    .registry()
                    .crdt(crdt, LWWRegister.class);

                Assert.assertEquals(Option.some(new TestLong(total - 1)), register.value());
            }

            printQps("CRDT async", total, start, end);
        }
    }

    private void awaitResourceAllocate(long crdt) throws Exception {
        // await bucket allocation
        while (true) {
            boolean allocated = true;
            for (CrdtService crdtService : crdtServices) {
                if (crdtService.routingTable().bucket(crdt).replicas().size() < nodes.size()) {
                    logger.info("await replica count: {}", crdtService.routingTable().bucket(crdt).replicas().size());
                    allocated = false;
                    break;
                } else if (crdtService.bucket(crdt) == null) {
                    logger.info("await bucket");
                    allocated = false;
                    break;
                } else if (crdtService.bucket(crdt).registry().crdtOpt(crdt).isEmpty()) {
                    logger.info("await crdt");
                    allocated = false;
                    break;
                }
            }
            if (allocated) {
                logger.info("await allocation done");
                break;
            }
            Thread.sleep(1000);
        }
    }

    public static class TestModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(ClusterRaftTest.RegisterClient.class).asEagerSingleton();

            Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);
            streamableBinder.addBinding().toInstance(of(TestLong.class, TestLong::new, 900000));
        }
    }

    public static class TestLong implements Streamable {
        private final long value;

        public TestLong(long value) {
            this.value = value;
        }

        public TestLong(StreamInput stream) {
            this.value = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeLong(value);
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
            return "TestLong{" +
                "value=" + value +
                '}';
        }
    }
}
