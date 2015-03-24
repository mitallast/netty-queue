package org.mitallast.queue.queue.transactional.memory;

import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.AbstractQueueComponent;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.stats.QueueStats;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class MemoryTransactionalQueueService extends AbstractQueueComponent implements TransactionalQueueService {

    private final ConcurrentHashMap<UUID, MessageEntry> messageMap;

    public MemoryTransactionalQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
        messageMap = new ConcurrentHashMap<>();
    }

    @Override
    protected void doStart() throws QueueException {
        messageMap.clear();
    }

    @Override
    protected void doStop() throws QueueException {
        messageMap.clear();
    }

    @Override
    protected void doClose() throws QueueException {
        messageMap.clear();
    }

    @Override
    public QueueTransaction transaction(String id) throws IOException {
        return new MemoryQueueTransaction(id, this);
    }

    @Override
    public QueueMessage get(UUID uuid) throws IOException {
        MessageEntry messageEntry = messageMap.get(uuid);
        if (messageEntry != null) {
            return messageEntry.queueMessage;
        }
        return null;
    }

    @Override
    public QueueMessage lock(UUID uuid) throws IOException {
        MessageEntry messageEntry = messageMap.get(uuid);
        if (messageEntry != null && messageEntry.setLockedStatus()) {
            return messageEntry.queueMessage;
        }
        return null;
    }

    @Override
    public QueueMessage lockAndPop() throws IOException {
        Iterator<MessageEntry> iterator = messageMap.values().iterator();
        MessageEntry messageEntry;
        while (iterator.hasNext()) {
            messageEntry = iterator.next();
            if (messageEntry.setLockedStatus()) {
                return messageEntry.queueMessage;
            }
        }
        return null;
    }

    @Override
    public QueueMessage unlockAndDelete(UUID uuid) throws IOException {
        MessageEntry messageEntry = messageMap.get(uuid);
        if (messageEntry != null && messageEntry.setUnlockDeleted()) {
            messageMap.remove(uuid);
            return messageEntry.queueMessage;
        }
        return null;
    }

    @Override
    public QueueMessage unlockAndRollback(UUID uuid) throws IOException {
        MessageEntry messageEntry = messageMap.get(uuid);
        if (messageEntry != null && messageEntry.setUnlockQueued()) {
            return messageEntry.queueMessage;
        }
        return null;
    }

    @Override
    public boolean push(QueueMessage queueMessage) throws IOException {
        MessageEntry messageEntry = new MessageEntry(queueMessage, MessageStatus.QUEUED);
        if (messageMap.putIfAbsent(queueMessage.getUuid(), messageEntry) != null) {
            throw new QueueMessageUuidDuplicateException(queueMessage.getUuid());
        }
        return true;
    }

    @Override
    public long size() {
        return messageMap.size();
    }

    @Override
    public QueueStats stats() throws IOException {
        return null;
    }

    enum MessageStatus {QUEUED, LOCKED, DELETED}

    private static class MessageEntry {

        final static AtomicReferenceFieldUpdater<MessageEntry, MessageStatus> statusUpdater =
            AtomicReferenceFieldUpdater.newUpdater(MessageEntry.class, MessageStatus.class, "messageStatus");

        final QueueMessage queueMessage;
        volatile MessageStatus messageStatus;

        MessageEntry(QueueMessage queueMessage, MessageStatus messageStatus) {
            this.queueMessage = queueMessage;
            this.messageStatus = messageStatus;
        }

        boolean setLockedStatus() {
            return statusUpdater.compareAndSet(this, MessageStatus.QUEUED, MessageStatus.LOCKED);
        }

        boolean setUnlockQueued() {
            return statusUpdater.compareAndSet(this, MessageStatus.LOCKED, MessageStatus.QUEUED);
        }

        boolean setUnlockDeleted() {
            return statusUpdater.compareAndSet(this, MessageStatus.LOCKED, MessageStatus.DELETED);
        }
    }
}
