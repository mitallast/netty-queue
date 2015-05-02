package org.mitallast.queue.client;

import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

public interface QueuesClient {

    SmartFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request);

    default void queuesStatsRequest(QueuesStatsRequest request, Listener<QueuesStatsResponse> listener) {
        queuesStatsRequest(request).on(listener);
    }

    SmartFuture<CreateQueueResponse> createQueue(CreateQueueRequest request);

    default void createQueue(CreateQueueRequest request, Listener<CreateQueueResponse> listener) {
        createQueue(request).on(listener);
    }

    SmartFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request);

    default void deleteQueue(DeleteQueueRequest request, Listener<DeleteQueueResponse> listener) {
        deleteQueue(request).on(listener);
    }
}
