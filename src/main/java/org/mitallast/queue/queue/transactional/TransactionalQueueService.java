package org.mitallast.queue.queue.transactional;

import org.mitallast.queue.queue.QueueComponent;
import org.mitallast.queue.queues.stats.QueueStats;

import java.io.IOException;

public interface TransactionalQueueService extends TransactionalQueueComponent, QueueComponent {

    public QueueTransaction transaction(String id) throws IOException;

    public QueueStats stats() throws IOException;
}
