package org.mitallast.queue.client.base;

public interface Client {

    QueuesClient queues();

    QueueClient queue();
}
