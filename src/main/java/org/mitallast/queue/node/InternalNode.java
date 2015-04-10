package org.mitallast.queue.node;

import com.google.inject.Injector;
import org.mitallast.queue.action.ActionModule;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.client.ClientModule;
import org.mitallast.queue.client.local.LocalClient;
import org.mitallast.queue.common.component.Lifecycle;
import org.mitallast.queue.common.component.ModulesBuilder;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.transactional.InternalTransactionalQueuesService;
import org.mitallast.queue.queues.transactional.TransactionalQueuesModule;
import org.mitallast.queue.rest.RestModule;
import org.mitallast.queue.rest.transport.HttpServer;
import org.mitallast.queue.transport.TransportModule;
import org.mitallast.queue.transport.client.TransportClient;
import org.mitallast.queue.transport.transport.TransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalNode implements Node {

    private final static Logger logger = LoggerFactory.getLogger(InternalNode.class);

    private final Lifecycle lifecycle = new Lifecycle();

    private final Settings settings;

    private final Injector injector;

    public InternalNode(Settings settings) {
        this.settings = settings;

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
    public Client client() {
        return injector.getInstance(Client.class);
    }

    @Override
    public TransportClient transportClient() {
        return injector.getInstance(TransportClient.class);
    }

    @Override
    public LocalClient localClient() {
        return injector.getInstance(LocalClient.class);
    }

    @Override
    public Settings settings() {
        return settings;
    }

    @Override
    public Node start() {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        logger.info("starting...");
        injector.getInstance(InternalTransactionalQueuesService.class).start();
        injector.getInstance(HttpServer.class).start();
        injector.getInstance(TransportServer.class).start();
        injector.getInstance(TransportClient.class).start();
        logger.info("started");
        return this;
    }

    @Override
    public Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        logger.info("stopping...");
        injector.getInstance(HttpServer.class).stop();
        injector.getInstance(TransportClient.class).stop();
        injector.getInstance(TransportServer.class).stop();
        injector.getInstance(InternalTransactionalQueuesService.class).stop();
        logger.info("stopped");
        return this;
    }

    @Override
    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        logger.info("closing...");
        injector.getInstance(HttpServer.class).close();
        injector.getInstance(TransportServer.class).close();
        injector.getInstance(InternalTransactionalQueuesService.class).close();
        logger.info("closed");
    }

    @Override
    public boolean isClosed() {
        return lifecycle.closed();
    }

    @Override
    public Injector injector() {
        return injector;
    }
}
