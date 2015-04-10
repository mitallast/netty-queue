package org.mitallast.queue.client.base;

import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

public interface QueuesClient {

    SmartFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request);

    SmartFuture<CreateQueueResponse> createQueue(CreateQueueRequest request);

    SmartFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request);
}
