package org.mitallast.queue.node;

import com.google.inject.Injector;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.action.ActionModule;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.client.ClientModule;
import org.mitallast.queue.client.local.LocalClient;
import org.mitallast.queue.cluster.DiscoveryNode;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.component.ModulesBuilder;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.transactional.InternalTransactionalQueuesService;
import org.mitallast.queue.queues.transactional.TransactionalQueuesModule;
import org.mitallast.queue.rest.RestModule;
import org.mitallast.queue.rest.transport.HttpServer;
import org.mitallast.queue.transport.TransportModule;
import org.mitallast.queue.transport.TransportServer;
import org.mitallast.queue.transport.netty.NettyTransportServer;
import org.mitallast.queue.transport.netty.NettyTransportService;

public class InternalNode extends AbstractLifecycleComponent implements Node {

    private final Injector injector;

    public InternalNode(Settings settings) {
        super(settings);

        logger.info("initializing...");

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new TransactionalQueuesModule(settings));
        modules.add(new ActionModule());
        modules.add(new ClientModule());
        modules.add(new RestModule());
        modules.add(new TransportModule());

        injector = modules.createInjector();

        logger.info("initialized");
    }

    @Override
    public Client localClient() {
        return injector.getInstance(LocalClient.class);
    }

    @Override
    public DiscoveryNode localNode() {
        return injector.getInstance(TransportServer.class).localNode();
    }

    @Override
    public Settings settings() {
        return settings;
    }

    @Override
    public Injector injector() {
        return injector;
    }

    @Override
    protected void doStart() throws QueueException {
        injector.getInstance(InternalTransactionalQueuesService.class).start();
        injector.getInstance(NettyTransportService.class).start();
        injector.getInstance(NettyTransportServer.class).start();
        injector.getInstance(HttpServer.class).start();
    }

    @Override
    protected void doStop() throws QueueException {
        injector.getInstance(HttpServer.class).stop();
        injector.getInstance(NettyTransportServer.class).stop();
        injector.getInstance(NettyTransportService.class).stop();
        injector.getInstance(InternalTransactionalQueuesService.class).stop();
    }

    @Override
    protected void doClose() throws QueueException {
        injector.getInstance(HttpServer.class).close();
        injector.getInstance(NettyTransportServer.class).close();
        injector.getInstance(NettyTransportService.class).close();
        injector.getInstance(InternalTransactionalQueuesService.class).close();
    }
}
