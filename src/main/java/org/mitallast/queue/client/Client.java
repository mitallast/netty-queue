package org.mitallast.queue.client;

public interface Client {

    QueuesClient queues();

    QueueClient queue();
}
