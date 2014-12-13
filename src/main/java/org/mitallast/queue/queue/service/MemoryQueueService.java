package org.mitallast.queue.queue.service;

import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MemoryQueueService extends AbstractQueueService {

    private final ConcurrentLinkedQueue<QueueMessage> queueMessages = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<UUID, QueueMessage> queueMessageMap = new ConcurrentHashMap<>();

    public MemoryQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
    }

    @Override
    protected void doStart() throws QueueException {
        queueMessageMap.clear();
        queueMessages.clear();
    }

    @Override
    protected void doStop() throws QueueException {
        queueMessageMap.clear();
        queueMessages.clear();
    }

    @Override
    protected void doClose() throws QueueException {

    }

    @Override
    public void enqueue(QueueMessage message) {
        UUID uuid = message.getUuid();
        if (uuid == null) {
            uuid = UUIDs.generateRandom();
            message.setUuid(uuid);
        }
        if (queueMessageMap.putIfAbsent(uuid, message) == null) {
            queueMessages.add(message);
        } else {
            throw new QueueMessageUuidDuplicateException(message.getUuid());
        }
    }

    @Override
    public QueueMessage dequeue() {
        QueueMessage message;
        while ((message = queueMessages.poll()) != null) {
            if (queueMessageMap.remove(message.getUuid()) != null) {
                return message;
            }
        }
        return null;
    }

    @Override
    public QueueMessage peek() {
        return queueMessages.peek();
    }

    @Override
    public QueueMessage get(UUID uuid) {
        return queueMessageMap.get(uuid);
    }

    @Override
    public void delete(UUID uuid) {
        queueMessageMap.remove(uuid);
    }

    @Override
    public long size() {
        return queueMessageMap.size();
    }

    @Override
    public void removeQueue() {
        queueMessageMap.clear();
        queueMessages.clear();
    }
}
