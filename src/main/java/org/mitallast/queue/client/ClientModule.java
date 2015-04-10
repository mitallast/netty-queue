package org.mitallast.queue.client;

import com.google.inject.AbstractModule;
import org.mitallast.queue.client.local.LocalClient;
import org.mitallast.queue.client.local.LocalQueueClient;
import org.mitallast.queue.client.local.LocalQueuesClient;

public class ClientModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(QueuesClient.class).asEagerSingleton();
        bind(QueueClient.class).asEagerSingleton();
        bind(Client.class).asEagerSingleton();

        bind(LocalQueueClient.class).asEagerSingleton();
        bind(LocalQueuesClient.class).asEagerSingleton();
        bind(LocalClient.class).asEagerSingleton();
    }
}
