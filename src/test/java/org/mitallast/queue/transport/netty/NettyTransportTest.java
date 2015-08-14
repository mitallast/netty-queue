package org.mitallast.queue.transport.netty;

import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.action.ActionStreamInitializer;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.common.component.ComponentModule;
import org.mitallast.queue.common.component.LifecycleService;
import org.mitallast.queue.common.component.ModulesBuilder;
import org.mitallast.queue.common.stream.StreamModule;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportServer;
import org.mitallast.queue.transport.TransportService;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class NettyTransportTest extends BaseIntegrationTest {

    @Mock
    private TransportController transportController;

    @Inject
    private NettyTransportServer transportServer;

    @Inject
    private NettyTransportService transportService;

    @Inject
    private LifecycleService lifecycleService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        new ModulesBuilder()
            .add(new ComponentModule(settings()))
            .add(new StreamModule())
            .add(new TestModule())
            .createInjector()
            .injectMembers(this);

        lifecycleService.start();
    }

    @After
    public void tearDown() throws Exception {
        lifecycleService.stop();
        lifecycleService.close();
    }

    @Test(expected = IOException.class)
    public void testName() throws Exception {
        DiscoveryNode node = transportServer.localNode();
        HostAndPort address = HostAndPort.fromParts("localhost", node.address().getPort());
        transportService.connectToNode(address);
        CompletableFuture<ActionResponse> future = transportService.client(address).send(QueuesStatsRequest.builder().build());
        transportServer.stop(); // emulate disconnect
        try {
            future.get();
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), IOException.class);
            throw e;
        }
    }

    private class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(ActionStreamInitializer.class).asEagerSingleton();
            bind(TransportController.class).toInstance(transportController);
            bind(NettyTransportServer.class).asEagerSingleton();
            bind(NettyTransportService.class).asEagerSingleton();
            bind(TransportServer.class).to(NettyTransportServer.class);
            bind(TransportService.class).to(NettyTransportService.class);
        }
    }
}
