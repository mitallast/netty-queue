package org.mitallast.queue.queues.transactional;

import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.stats.QueueStats;
import org.mitallast.queue.queues.stats.QueuesStats;

import java.io.IOException;
import java.util.Set;

public interface TransactionalQueuesService {

    boolean hasQueue(String name);

    Set<String> queues();

    QueuesStats stats() throws IOException;

    QueueStats stats(String name) throws IOException;

    TransactionalQueueService queue(String name);

    TransactionalQueueService createQueue(String name, Settings queueSettings) throws IOException;

    void deleteQueue(String name, String reason) throws IOException;
}
