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
import org.mitallast.queue.crdt.routing.ResourceType;
import org.mitallast.queue.crdt.routing.fsm.AddResource;
import org.mitallast.queue.raft.ClusterRaftTest;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

import static org.mitallast.queue.common.stream.StreamableRegistry.of;

public class ClusterCrdtTest extends BaseClusterTest {

    private ImmutableList<CrdtService> crdtServices;
    private ImmutableList<DiscoveryNode> discoveryNodes;

    @Override
    public void setUpNodes() throws Exception {
        super.setUpNodes();
        crdtServices = Immutable.map(nodes, n -> n.injector().getInstance(CrdtService.class));
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

        long total = 400000;

        for (long id = 0; id < nodesCount; id++) {
            raft.get(0).apply(new ClientMessage(discoveryNodes.get(0), new AddResource(id, ResourceType.LWWRegister)));
            awaitResourceAllocate(id);

            long expected = (total / nodesCount + (nodesCount % 2));

            long start = System.currentTimeMillis();
            for (long i = 0; i < total; i++) {
                crdtServices.get((int) (i % nodesCount))
                    .bucket(id)
                    .registry()
                    .crdt(id, LWWRegister.class)
                    .assign(new TestLong(i));
            }
            for (int w = 0; w < 10; w++) {
                if (expected != crdtServices.get(1).bucket(id).vclock().get(discoveryNodes.get(0))) {
                    Thread.sleep(10);
                    continue;
                }
                if (expected != crdtServices.get(2).bucket(id).vclock().get(discoveryNodes.get(0))) {
                    Thread.sleep(10);
                    continue;
                }
                break;
            }
            long end = System.currentTimeMillis();

            Assert.assertEquals(expected, crdtServices.get(1).bucket(id).vclock().get(discoveryNodes.get(0)));
            Assert.assertEquals(expected, crdtServices.get(2).bucket(id).vclock().get(discoveryNodes.get(0)));

            printQps("CRDT async", total, start, end);
        }
    }

    private void awaitResourceAllocate(long id) throws Exception {
        // await bucket allocation
        while (true) {
            boolean allocated = true;
            for (CrdtService crdtService : crdtServices) {
                if (crdtService.routingTable().bucket(id).members().size() < nodesCount) {
                    allocated = false;
                }
                if (!crdtService.bucket(id).registry().crdtOpt(id).isPresent()) {
                    allocated = false;
                }
            }
            if (allocated) {
                logger.info("await allocation done");
                break;
            }
            logger.info("await allocation");
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

        public TestLong(StreamInput stream) throws IOException {
            this.value = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(value);
        }
    }
}
