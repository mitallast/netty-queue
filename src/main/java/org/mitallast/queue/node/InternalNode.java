package org.mitallast.queue.node;

import com.google.inject.Injector;
import org.mitallast.queue.client.ClientModule;
import org.mitallast.queue.common.component.Lifecycle;
import org.mitallast.queue.common.component.ModulesBuilder;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.InternalQueuesService;
import org.mitallast.queue.queues.QueuesModule;
import org.mitallast.queue.rest.RestModule;
import org.mitallast.queue.transport.TransportModule;
import org.mitallast.queue.transport.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalNode implements Node {

    private final static Logger logger = LoggerFactory.getLogger(Node.class);

    private final Lifecycle lifecycle = new Lifecycle();

    private final Settings settings;

    private final Injector injector;

    public InternalNode(Settings settings) {
        this.settings = settings;

        logger.info("initializing...");

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new QueuesModule(this.settings));
        modules.add(new ClientModule());
        modules.add(new RestModule());
        modules.add(new TransportModule());

        injector = modules.createInjector();

        logger.info("initialized");
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
        injector.getInstance(InternalQueuesService.class).start();
        injector.getInstance(HttpServer.class).start();
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
        injector.getInstance(InternalQueuesService.class).stop();
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
        injector.getInstance(InternalQueuesService.class).close();
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
