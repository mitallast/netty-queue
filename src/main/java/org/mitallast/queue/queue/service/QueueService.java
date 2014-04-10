package org.mitallast.queue.queue.service;

import org.mitallast.queue.queue.QueueComponent;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueType;

public interface QueueService<Message> extends QueueComponent {
    void enqueue(QueueMessage<Message> message);

    QueueMessage<Message> dequeue();

    long size();

    QueueType type();

    void deleteQueue();

    boolean isSupported(QueueMessage message);
}
