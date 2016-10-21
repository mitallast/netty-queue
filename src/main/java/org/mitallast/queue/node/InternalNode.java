package org.mitallast.queue.node;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.component.ComponentModule;
import org.mitallast.queue.common.component.LifecycleService;
import org.mitallast.queue.common.component.ModulesBuilder;
import org.mitallast.queue.common.stream.StreamModule;
import org.mitallast.queue.raft.RaftModule;
import org.mitallast.queue.rest.RestModule;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportModule;
import org.mitallast.queue.transport.TransportServer;

import java.io.IOException;

public class InternalNode extends AbstractLifecycleComponent implements Node {

    private final Injector injector;

    public InternalNode(Config config) {
        super(prepareConfig(config), Node.class);

        logger.info("initializing...");

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new ComponentModule(this.config));
        modules.add(new StreamModule());
        modules.add(new TransportModule());
        if (config.getBoolean("rest.enabled")) {
            modules.add(new RestModule());
        }
        if (config.getBoolean("raft.enabled")) {
            modules.add(new RaftModule());
        }
        injector = modules.createInjector();

        logger.info("initialized");
    }

    @Override
    public DiscoveryNode localNode() {
        return injector.getInstance(TransportServer.class).localNode();
    }

    @Override
    public Config config() {
        return config;
    }

    @Override
    public Injector injector() {
        return injector;
    }

    @Override
    protected void doStart() throws IOException {
        injector.getInstance(LifecycleService.class).start();
    }

    @Override
    protected void doStop() throws IOException {
        injector.getInstance(LifecycleService.class).stop();
    }

    @Override
    protected void doClose() throws IOException {
        injector.getInstance(LifecycleService.class).close();
    }

    private static Config prepareConfig(Config config) {
        String name = UUIDs.generateRandom().toString().substring(0, 8);
        Config fallback = ConfigFactory.parseMap(ImmutableMap.of("node.name", name));
        return config.withFallback(fallback);
    }
}
