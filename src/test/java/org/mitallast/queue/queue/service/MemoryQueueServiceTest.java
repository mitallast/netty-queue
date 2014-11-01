package org.mitallast.queue.queue.service;

import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;

public class MemoryQueueServiceTest extends BaseQueueServiceTest<MemoryQueueService> {

    @Override
    protected MemoryQueueService createQueueService(Settings settings, Settings queueSettings, Queue queue) {
        return new MemoryQueueService(settings, queueSettings, queue);
    }
}
