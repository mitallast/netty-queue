package org.mitallast.queue.client;

import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;

import java.util.concurrent.CompletableFuture;

public interface QueuesClient {

    CompletableFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request);

    CompletableFuture<CreateQueueResponse> createQueue(CreateQueueRequest request);

    CompletableFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request);
}
