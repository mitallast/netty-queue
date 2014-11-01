package org.mitallast.queue.queue.service;

import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;

public class LevelDbQueueServiceTest extends BaseQueueServiceTest<LevelDbQueueService> {

    @Override
    protected LevelDbQueueService createQueueService(Settings settings, Settings queueSettings, Queue queue) {
        return new LevelDbQueueService(settings, queueSettings, queue);
    }
}
