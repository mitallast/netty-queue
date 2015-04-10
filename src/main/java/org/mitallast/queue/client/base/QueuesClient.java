package org.mitallast.queue.client.base;

import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

public interface QueuesClient {

    SmartFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request) throws IOException;

    SmartFuture<CreateQueueResponse> createQueue(CreateQueueRequest request) throws IOException;

    SmartFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request) throws IOException;
}
