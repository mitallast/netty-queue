package org.mitallast.queue.client.base;

import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

public interface QueuesClient {

    SmartFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request) throws IOException;

    void queuesStatsRequest(QueuesStatsRequest request, Listener<QueuesStatsResponse> listener);

    SmartFuture<CreateQueueResponse> createQueue(CreateQueueRequest request) throws IOException;

    void createQueue(CreateQueueRequest request, Listener<CreateQueueResponse> listener);

    SmartFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request) throws IOException;

    void deleteQueue(DeleteQueueRequest request, Listener<DeleteQueueResponse> listener);
}
