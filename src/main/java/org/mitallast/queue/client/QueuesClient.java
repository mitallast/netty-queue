package org.mitallast.queue.client;

import com.google.inject.Inject;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.create.CreateQueueAction;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.remove.RemoveQueueAction;
import org.mitallast.queue.action.queues.remove.RemoveQueueRequest;
import org.mitallast.queue.action.queues.remove.RemoveQueueResponse;
import org.mitallast.queue.action.queues.stats.QueueStatsAction;
import org.mitallast.queue.action.queues.stats.QueueStatsRequest;
import org.mitallast.queue.action.queues.stats.QueueStatsResponse;

public class QueuesClient {

    private final QueueStatsAction queuesStatsAction;
    private final CreateQueueAction createQueueAction;
    private final RemoveQueueAction removeQueueAction;

    @Inject
    public QueuesClient(QueueStatsAction queuesStatsAction, CreateQueueAction createQueueAction, RemoveQueueAction removeQueueAction) {
        this.queuesStatsAction = queuesStatsAction;
        this.createQueueAction = createQueueAction;
        this.removeQueueAction = removeQueueAction;
    }

    public void queuesStatsRequest(QueueStatsRequest request, ActionListener<QueueStatsResponse> listener) {
        queuesStatsAction.execute(request, listener);
    }

    public void createQueue(CreateQueueRequest request, ActionListener<CreateQueueResponse> listener) {
        createQueueAction.execute(request, listener);
    }

    public void removeQueue(RemoveQueueRequest request, ActionListener<RemoveQueueResponse> listener) {
        removeQueueAction.execute(request, listener);
    }
}
