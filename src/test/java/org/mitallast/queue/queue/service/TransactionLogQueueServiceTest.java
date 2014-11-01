package org.mitallast.queue.queue.service;

import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;

public class TransactionLogQueueServiceTest extends BaseQueueServiceTest<TransactionLogQueueService> {
    @Override
    protected TransactionLogQueueService createQueueService(Settings settings, Settings queueSettings, Queue queue) {
        return new TransactionLogQueueService(settings, queueSettings, queue);
    }
}
