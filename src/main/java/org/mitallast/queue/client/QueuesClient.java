package org.mitallast.queue.client;

import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.create.CreateQueueAction;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.remove.RemoveQueueAction;
import org.mitallast.queue.action.queues.remove.RemoveQueueRequest;
import org.mitallast.queue.action.queues.remove.RemoveQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsAction;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;

import java.util.concurrent.ExecutionException;

public class QueuesClient {

    private final QueuesStatsAction queuesStatsAction;
    private final CreateQueueAction createQueueAction;
    private final RemoveQueueAction removeQueueAction;

    public QueuesClient(QueuesStatsAction queuesStatsAction, CreateQueueAction createQueueAction, RemoveQueueAction removeQueueAction) {
        this.queuesStatsAction = queuesStatsAction;
        this.createQueueAction = createQueueAction;
        this.removeQueueAction = removeQueueAction;
    }

    public QueuesStatsResponse queuesStatsRequest(QueuesStatsRequest request) {
        try {
            return queuesStatsAction.execute(request).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new QueueRuntimeException(e);
        }
    }

    public void queuesStatsRequest(QueuesStatsRequest request, ActionListener<QueuesStatsResponse> listener) {
        queuesStatsAction.execute(request, listener);
    }

    public CreateQueueResponse createQueue(CreateQueueRequest request) {
        try {
            return createQueueAction.execute(request).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new QueueRuntimeException(e);
        }
    }

    public void createQueue(CreateQueueRequest request, ActionListener<CreateQueueResponse> listener) {
        createQueueAction.execute(request, listener);
    }

    public RemoveQueueResponse removeQueue(RemoveQueueRequest request) {
        try {
            return removeQueueAction.execute(request).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new QueueRuntimeException(e);
        }
    }

    public void removeQueue(RemoveQueueRequest request, ActionListener<RemoveQueueResponse> listener) {
        removeQueueAction.execute(request, listener);
    }
}
