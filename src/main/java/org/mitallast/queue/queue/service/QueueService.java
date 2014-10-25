package org.mitallast.queue.queue.service;

import org.mitallast.queue.queue.QueueComponent;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queues.stats.QueueStats;

public interface QueueService<Message> extends QueueComponent {

    void enqueue(QueueMessage<Message> message);

    QueueMessage<Message> dequeue();

    QueueMessage<Message> peek();

    long size();

    void removeQueue();

    boolean isSupported(QueueMessage message);

    QueueStats stats();
}
