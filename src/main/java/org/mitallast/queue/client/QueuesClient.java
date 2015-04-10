package org.mitallast.queue.client;

import com.google.inject.Inject;
import org.mitallast.queue.action.queues.create.CreateQueueAction;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.delete.DeleteQueueAction;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsAction;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

public class QueuesClient {

    private final QueuesStatsAction queuesStatsAction;
    private final CreateQueueAction createQueueAction;
    private final DeleteQueueAction deleteQueueAction;

    @Inject
    public QueuesClient(QueuesStatsAction queuesStatsAction, CreateQueueAction createQueueAction, DeleteQueueAction deleteQueueAction) {
        this.queuesStatsAction = queuesStatsAction;
        this.createQueueAction = createQueueAction;
        this.deleteQueueAction = deleteQueueAction;
    }

    public SmartFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request) {
        return queuesStatsAction.execute(request);
    }

    public void queuesStatsRequest(QueuesStatsRequest request, Listener<QueuesStatsResponse> listener) {
        queuesStatsAction.execute(request, listener);
    }

    public SmartFuture<CreateQueueResponse> createQueue(CreateQueueRequest request) {
        return createQueueAction.execute(request);
    }

    public void createQueue(CreateQueueRequest request, Listener<CreateQueueResponse> listener) {
        createQueueAction.execute(request, listener);
    }

    public SmartFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request) {
        return deleteQueueAction.execute(request);
    }

    public void deleteQueue(DeleteQueueRequest request, Listener<DeleteQueueResponse> listener) {
        deleteQueueAction.execute(request, listener);
    }
}
