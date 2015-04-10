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
import org.mitallast.queue.client.base.QueuesClient;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

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
    public SmartFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request) throws IOException {
        return queuesStatsAction.execute(request);
    }

    @Override
    public void queuesStatsRequest(QueuesStatsRequest request, Listener<QueuesStatsResponse> listener) {
        queuesStatsAction.execute(request, listener);
    }

    @Override
    public SmartFuture<CreateQueueResponse> createQueue(CreateQueueRequest request) throws IOException {
        return createQueueAction.execute(request);
    }

    @Override
    public void createQueue(CreateQueueRequest request, Listener<CreateQueueResponse> listener) {
        createQueueAction.execute(request, listener);
    }

    @Override
    public SmartFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request) throws IOException {
        return deleteQueueAction.execute(request);
    }

    @Override
    public void deleteQueue(DeleteQueueRequest request, Listener<DeleteQueueResponse> listener) {
        deleteQueueAction.execute(request, listener);
    }
}
