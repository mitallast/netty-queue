package org.mitallast.queue.crdt;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseClusterTest;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.vclock.VectorClock;
import org.mitallast.queue.raft.ClusterRaftTest;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

import static org.mitallast.queue.common.stream.StreamableRegistry.of;

public class ClusterCrdtTest extends BaseClusterTest {

    private ImmutableList<CrdtService> crdtServices;
    private ImmutableList<VectorClock> vclocks;
    private ImmutableList<DiscoveryNode> discoveryNodes;

    @Override
    public void setUpNodes() throws Exception {
        super.setUpNodes();
        crdtServices = Immutable.map(nodes, n -> n.injector().getInstance(CrdtService.class));
        vclocks = Immutable.map(nodes, n -> n.injector().getInstance(VectorClock.class));
        discoveryNodes = Immutable.map(nodes, n -> n.injector().getInstance(ClusterDiscovery.class).self());
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

        for (int id = 0; id < 9; id++) {

            for (int i = 0; i < nodesCount; i++) {
                crdtServices.get(i).createLWWRegister(id);
            }

            long expected = (total / nodesCount + (nodesCount % 2)) * (id + 1);

            long start = System.currentTimeMillis();
            for (long i = 0; i < total; i++) {
                crdtServices.get((int) (i % nodesCount))
                    .crdt(id, LWWRegister.class)
                    .assign(new TestLong(i));
            }
            for (int w = 0; w < 10; w++) {
                if (expected != vclocks.get(1).get(discoveryNodes.get(0))) {
                    Thread.sleep(10);
                    continue;
                }
//                if (expected != vclocks.get(2).get(discoveryNodes.get(0))) {
//                    Thread.sleep(10);
//                    continue;
//                }
                break;
            }
            long end = System.currentTimeMillis();

            Assert.assertEquals(expected, vclocks.get(1).get(discoveryNodes.get(0)));
//            Assert.assertEquals(expected, vclocks.get(2).get(discoveryNodes.get(0)));

            printQps("CRDT async", total, start, end);
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

        public TestLong(StreamInput stream) throws IOException {
            this.value = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(value);
        }
    }
}
