package org.mitallast.queue.queues;

import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueType;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queues.stats.QueueStats;
import org.mitallast.queue.queues.stats.QueuesStats;

import java.util.Set;

public interface QueuesService {
    boolean hasQueue(String name);

    Set<String> queues();

    QueuesStats stats();

    QueueStats stats(String name);

    QueueService queue(String name);

    QueueService createQueue(String name, QueueType type, Settings queueSettings);

    void deleteQueue(String name, String reason);
}
