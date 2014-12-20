package org.mitallast.queue.queue.transactional;

import org.mitallast.queue.queue.QueueComponent;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queues.stats.QueueStats;

import java.io.IOException;
import java.util.UUID;

public interface TransactionalQueueService extends QueueComponent {

    public QueueTransaction transaction(String id) throws IOException;

    public QueueMessage lock(UUID uuid) throws IOException;

    public QueueMessage lockAndPop() throws IOException;

    public QueueMessage unlockAndDelete(UUID uuid) throws IOException;

    public QueueMessage unlockAndRollback(UUID uuid) throws IOException;

    public void push(QueueMessage queueMessage) throws IOException;

    long size();

    public QueueStats stats() throws IOException;
}
