package org.mitallast.queue.client;

import com.google.inject.AbstractModule;
import org.mitallast.queue.client.base.Client;
import org.mitallast.queue.client.base.QueueClient;
import org.mitallast.queue.client.base.QueueTransactionalClient;
import org.mitallast.queue.client.base.QueuesClient;
import org.mitallast.queue.client.local.LocalClient;
import org.mitallast.queue.client.local.LocalQueueClient;
import org.mitallast.queue.client.local.LocalQueueTransactionalClient;
import org.mitallast.queue.client.local.LocalQueuesClient;

public class ClientModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(QueueClient.class).to(LocalQueueClient.class).asEagerSingleton();
        bind(QueueTransactionalClient.class).to(LocalQueueTransactionalClient.class).asEagerSingleton();
        bind(QueuesClient.class).to(LocalQueuesClient.class).asEagerSingleton();
        bind(Client.class).to(LocalClient.class).asEagerSingleton();
    }
}
