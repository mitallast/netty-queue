package org.mitallast.queue.client.local;

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
import org.mitallast.queue.client.QueuesClient;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

public class LocalQueuesClient implements QueuesClient {

    private final QueuesStatsAction queuesStatsAction;
    private final CreateQueueAction createQueueAction;
    private final DeleteQueueAction deleteQueueAction;

    @Inject
    public LocalQueuesClient(QueuesStatsAction queuesStatsAction, CreateQueueAction createQueueAction, DeleteQueueAction deleteQueueAction) {
        this.queuesStatsAction = queuesStatsAction;
        this.createQueueAction = createQueueAction;
        this.deleteQueueAction = deleteQueueAction;
    }

    @Override
    public SmartFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request) {
        return queuesStatsAction.execute(request);
    }

    @Override
    public SmartFuture<CreateQueueResponse> createQueue(CreateQueueRequest request) {
        return createQueueAction.execute(request);
    }

    @Override
    public SmartFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request) {
        return deleteQueueAction.execute(request);
    }
}
