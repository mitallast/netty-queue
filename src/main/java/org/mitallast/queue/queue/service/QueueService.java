package org.mitallast.queue.queue.service;

import org.mitallast.queue.queue.QueueComponent;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queues.stats.QueueStats;

import java.util.UUID;

public interface QueueService extends QueueComponent {

    void enqueue(QueueMessage message);

    QueueMessage dequeue();

    QueueMessage peek();

    QueueMessage get(UUID uuid);

    void delete(UUID uuid);

    long size();

    void removeQueue();

    QueueStats stats();
}
