package org.mitallast.queue.action.queue.stats;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class QueueStatsAction extends AbstractAction<QueueStatsRequest, QueueStatsResponse> {
    private TransactionalQueuesService queuesService;

    @Inject
    public QueueStatsAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(QueueStatsRequest request, CompletableFuture<QueueStatsResponse> listener) {
        if (!queuesService.hasQueue(request.queue())) {
            listener.completeExceptionally(new QueueMissingException(request.queue()));
        }
        TransactionalQueueService queueService = queuesService.queue(request.queue());
        try {
            listener.complete(QueueStatsResponse.builder()
                .setStats(queueService.stats())
                .build());
        } catch (IOException e) {
            listener.completeExceptionally(e);
        }
    }
}
