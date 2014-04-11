package org.mitallast.queue.client;

import com.google.inject.AbstractModule;

public class ClientModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(QueuesClient.class).asEagerSingleton();
        bind(QueueClient.class).asEagerSingleton();
        bind(Client.class).asEagerSingleton();
    }
}
