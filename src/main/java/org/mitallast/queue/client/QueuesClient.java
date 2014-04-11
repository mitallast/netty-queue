package org.mitallast.queue.client;

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

public class QueuesClient {

    private final QueuesStatsAction queuesStatsAction;
    private final CreateQueueAction createQueueAction;
    private final RemoveQueueAction removeQueueAction;

    public QueuesClient(QueuesStatsAction queuesStatsAction, CreateQueueAction createQueueAction, RemoveQueueAction removeQueueAction) {
        this.queuesStatsAction = queuesStatsAction;
        this.createQueueAction = createQueueAction;
        this.removeQueueAction = removeQueueAction;
    }

    public void queuesStatsRequest(QueuesStatsRequest request, ActionListener<QueuesStatsResponse> listener) {
        queuesStatsAction.execute(request, listener);
    }

    public void createQueue(CreateQueueRequest request, ActionListener<CreateQueueResponse> listener) {
        createQueueAction.execute(request, listener);
    }

    public void removeQueue(RemoveQueueRequest request, ActionListener<RemoveQueueResponse> listener) {
        removeQueueAction.execute(request, listener);
    }
}
