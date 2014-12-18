package org.mitallast.queue.queue.transactional.memory;

import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.AbstractQueueComponent;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.stats.QueueStats;

import java.io.IOException;
import java.util.UUID;

public class MemoryTransactionalQueueService extends AbstractQueueComponent implements TransactionalQueueService {

    public MemoryTransactionalQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
    }

    @Override
    protected void doStart() throws QueueException {

    }

    @Override
    protected void doStop() throws QueueException {

    }

    @Override
    protected void doClose() throws QueueException {

    }

    @Override
    public QueueTransaction transaction(String id) throws IOException {
        return null;
    }

    @Override
    public QueueTransaction transaction() throws IOException {
        return null;
    }

    @Override
    public QueueMessage lock(UUID uuid) throws IOException {
        return null;
    }

    @Override
    public QueueMessage lockAndPop() throws IOException {
        return null;
    }

    @Override
    public QueueMessage unlockAndDelete(UUID uuid) throws IOException {

        return null;
    }

    @Override
    public QueueMessage unlockAndRollback(UUID uuid) throws IOException {

        return null;
    }

    @Override
    public void push(QueueMessage queueMessage) throws IOException {

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public QueueStats stats() throws IOException {
        return null;
    }
}
