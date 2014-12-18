package org.mitallast.queue.queue.transactional.memory;

import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryQueueTransaction implements QueueTransaction {

    private final String id;
    private final TransactionalQueueService queueService;
    private final ConcurrentHashMap<UUID, MessageStatus> messageStatusMap;
    private final ConcurrentHashMap<UUID, QueueMessage> messageMap;
    private volatile TransactionStatus status;

    public MemoryQueueTransaction(String id, TransactionalQueueService queueService) {
        this.id = id;
        this.queueService = queueService;
        this.status = TransactionStatus.INIT;
        this.messageStatusMap = new ConcurrentHashMap<>();
        this.messageMap = new ConcurrentHashMap<>();
    }

    @Override
    public String id() throws IOException {
        return id;
    }

    @Override
    public synchronized void begin() throws IOException {
        assertStatus(TransactionStatus.INIT);
        this.status = TransactionStatus.BEGIN;
    }

    @Override
    public synchronized void commit() throws IOException {
        assertStatus(TransactionStatus.BEGIN);
        this.status = TransactionStatus.COMMIT;
        for (Map.Entry<UUID, MessageStatus> entry : messageStatusMap.entrySet()) {
            UUID uuid = entry.getKey();
            switch (entry.getValue()) {
                case PUSH:
                    queueService.push(messageMap.get(entry.getKey()));
                    break;
                case POP:
                    queueService.unlockAndDelete(uuid);
                    break;
                case DELETE:
                    queueService.unlockAndDelete(uuid);
                    break;
            }
            messageStatusMap.remove(entry.getKey());
            messageMap.remove(entry.getKey());
        }
    }

    @Override
    public synchronized void rollback() throws IOException {
        assertStatus(TransactionStatus.BEGIN);
        this.status = TransactionStatus.ROLLBACK;
        for (Map.Entry<UUID, MessageStatus> entry : messageStatusMap.entrySet()) {
            switch (entry.getValue()) {
                case PUSH:
                    // ignore
                    break;
                case POP:
                    queueService.unlockAndRollback(entry.getKey());
                    break;
                case DELETE:
                    queueService.unlockAndRollback(entry.getKey());
                    break;
            }
            messageStatusMap.remove(entry.getKey());
            messageMap.remove(entry.getKey());
        }
    }

    @Override
    public void push(QueueMessage queueMessage) throws IOException {
        assertStatus(TransactionStatus.BEGIN);
        this.messageStatusMap.put(queueMessage.getUuid(), MessageStatus.PUSH);
        this.messageMap.put(queueMessage.getUuid(), queueMessage);
    }

    @Override
    public QueueMessage pop() throws IOException {
        assertStatus(TransactionStatus.BEGIN);
        QueueMessage queueMessage = queueService.lockAndPop();
        messageStatusMap.put(queueMessage.getUuid(), MessageStatus.POP);
        messageMap.put(queueMessage.getUuid(), queueMessage);
        return queueMessage;
    }

    @Override
    public QueueMessage delete(UUID uuid) throws IOException {
        assertStatus(TransactionStatus.BEGIN);
        QueueMessage queueMessage = queueService.lock(uuid);
        this.messageStatusMap.put(uuid, MessageStatus.DELETE);
        this.messageMap.put(uuid, queueMessage);
        return queueMessage;
    }

    private void assertStatus(TransactionStatus expected) throws IOException {
        if (this.status != expected) {
            throw new IOException("Expected status [" + expected + "], actual " + this.status);
        }
    }

    enum TransactionStatus {INIT, BEGIN, COMMIT, ROLLBACK}

    enum MessageStatus {PUSH, POP, DELETE}
}
