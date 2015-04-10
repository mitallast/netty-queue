package org.mitallast.queue.client.base;

public interface Client {

    default void flush() {
    }

    QueuesClient queues();

    QueueClient queue();
}
