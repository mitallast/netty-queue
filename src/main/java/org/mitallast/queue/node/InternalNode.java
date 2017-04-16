package org.mitallast.queue.node;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.component.ComponentModule;
import org.mitallast.queue.common.component.LifecycleService;
import org.mitallast.queue.common.component.ModulesBuilder;
import org.mitallast.queue.common.events.EventBusModule;
import org.mitallast.queue.common.file.FileModule;
import org.mitallast.queue.common.json.JsonModule;
import org.mitallast.queue.common.stream.StreamModule;
import org.mitallast.queue.crdt.CrdtModule;
import org.mitallast.queue.crdt.rest.RestCrdtModule;
import org.mitallast.queue.raft.RaftModule;
import org.mitallast.queue.raft.rest.RaftRestModule;
import org.mitallast.queue.rest.RestModule;
import org.mitallast.queue.transport.TransportModule;

public class InternalNode extends AbstractLifecycleComponent implements Node {

    private final Config config;
    private final Injector injector;

    public InternalNode(Config conf, AbstractModule... plugins) {
        config = conf.withFallback(ConfigFactory.defaultReference());

        logger.info("initializing...");

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new ComponentModule(config));
        modules.add(new FileModule());
        modules.add(new StreamModule());
        modules.add(new JsonModule());
        modules.add(new EventBusModule());
        modules.add(new TransportModule());
        if (config.getBoolean("rest.enabled")) {
            modules.add(new RestModule());
        }
        if (config.getBoolean("raft.enabled")) {
            modules.add(new RaftModule());
            if (config.getBoolean("rest.enabled")) {
                modules.add(new RaftRestModule());
            }
            if (config.getBoolean("crdt.enabled")) {
                modules.add(new CrdtModule());
                if (config.getBoolean("rest.enabled")) {
                    modules.add(new RestCrdtModule());
                }
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
    protected void doStart() {
        injector.getInstance(LifecycleService.class).start();
    }

    @Override
    protected void doStop() {
        injector.getInstance(LifecycleService.class).stop();
    }

    @Override
    protected void doClose() {
        injector.getInstance(LifecycleService.class).close();
    }
}
