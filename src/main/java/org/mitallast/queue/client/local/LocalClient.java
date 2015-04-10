package org.mitallast.queue.client.local;

import com.google.inject.Inject;
import org.mitallast.queue.client.base.Client;
import org.mitallast.queue.client.base.QueueClient;
import org.mitallast.queue.client.base.QueuesClient;

public class LocalClient implements Client {

    private final LocalQueueClient localQueueClient;
    private final LocalQueuesClient localQueuesClient;

    @Inject
    public LocalClient(LocalQueuesClient localQueuesClient, LocalQueueClient localQueueClient) {
        this.localQueuesClient = localQueuesClient;
        this.localQueueClient = localQueueClient;
    }

    @Override
    public QueuesClient queues() {
        return localQueuesClient;
    }

    @Override
    public QueueClient queue() {
        return localQueueClient;
    }
}
