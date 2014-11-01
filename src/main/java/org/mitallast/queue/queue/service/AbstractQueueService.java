package org.mitallast.queue.queue.service;

import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.AbstractQueueComponent;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queues.stats.QueueStats;

public abstract class AbstractQueueService extends AbstractQueueComponent implements QueueService {

    public AbstractQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
    }

    @Override
    public QueueStats stats() {
        QueueStats stats = new QueueStats();
        stats.setQueue(queue);
        stats.setSize(size());
        return stats;
    }
}
