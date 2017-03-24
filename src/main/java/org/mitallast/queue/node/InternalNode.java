package org.mitallast.queue.node;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.component.ComponentModule;
import org.mitallast.queue.common.component.LifecycleService;
import org.mitallast.queue.common.component.ModulesBuilder;
import org.mitallast.queue.common.file.FileModule;
import org.mitallast.queue.common.proto.ProtoModule;
import org.mitallast.queue.raft.RaftModule;
import org.mitallast.queue.raft.rest.RaftRestModule;
import org.mitallast.queue.rest.RestModule;
import org.mitallast.queue.transport.TransportModule;

import java.io.IOException;

public class InternalNode extends AbstractLifecycleComponent implements Node {

    private final Injector injector;

    public InternalNode(Config config, AbstractModule... plugins) {
        super(prepareConfig(config), Node.class);

        logger.info("initializing...");

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new ComponentModule(this.config));
        modules.add(new FileModule());
        modules.add(new ProtoModule());
        modules.add(new TransportModule());
        if (this.config.getBoolean("rest.enabled")) {
            modules.add(new RestModule());
        }
        if (this.config.getBoolean("raft.enabled")) {
            modules.add(new RaftModule());
            if (this.config.getBoolean("rest.enabled")) {
                modules.add(new RaftRestModule());
            }
        }

        modules.add((Module[]) plugins);
        injector = modules.createInjector();

        logger.info("initialized");
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
        return config.withFallback(fallback).withFallback(ConfigFactory.defaultReference());
    }
}
