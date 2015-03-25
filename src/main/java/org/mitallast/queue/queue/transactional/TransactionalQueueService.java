package org.mitallast.queue.queue.transactional;

import org.mitallast.queue.queue.QueueComponent;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queues.stats.QueueStats;

import java.io.IOException;

public interface TransactionalQueueService extends TransactionalQueueComponent, QueueComponent {

    public QueueTransaction transaction(String id) throws IOException;

    public boolean push(QueueMessage queueMessage) throws IOException;

    public QueueStats stats() throws IOException;

    public void delete() throws IOException;
}
