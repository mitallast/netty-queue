package org.mitallast.queue.node;

import com.google.inject.Injector;
import org.mitallast.queue.action.ActionModule;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.client.ClientModule;
import org.mitallast.queue.client.local.LocalClient;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.component.ComponentModule;
import org.mitallast.queue.common.component.LifecycleService;
import org.mitallast.queue.common.component.ModulesBuilder;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamModule;
import org.mitallast.queue.common.strings.Strings;
import org.mitallast.queue.queues.transactional.TransactionalQueuesModule;
import org.mitallast.queue.rest.RestModule;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportModule;
import org.mitallast.queue.transport.TransportServer;

import java.io.IOException;

public class InternalNode extends AbstractLifecycleComponent implements Node {

    private final Injector injector;

    public InternalNode(Settings settings) {
        super(prepareSettings(settings));

        logger.info("initializing...");

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new ComponentModule(this.settings));
        modules.add(new StreamModule());
        modules.add(new TransactionalQueuesModule());
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

    private static Settings prepareSettings(Settings settings) {
        String name = settings.get("node.name");
        if (Strings.isEmpty(name)) {
            name = UUIDs.generateRandom().toString().substring(0, 8);
            settings = ImmutableSettings.builder()
                .put(settings)
                .put("node.name", name)
                .build();
        }
        return settings;
    }
}
