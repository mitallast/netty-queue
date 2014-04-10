package org.mitallast.queue.client;

public class Client {
    private final QueuesClient queuesClient;

    private final QueueClient queueClient;

    public Client(QueuesClient queuesClient, QueueClient queueClient) {
        this.queuesClient = queuesClient;
        this.queueClient = queueClient;
    }

    public QueuesClient queues() {
        return queuesClient;
    }

    public QueueClient queue() {
        return queueClient;
    }
}
