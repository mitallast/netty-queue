package org.mitallast.queue.action.queue.stats;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class QueueStatsAction extends AbstractAction<QueueStatsRequest, QueueStatsResponse> {

    public final static int ACTION_ID = 6;
    private TransactionalQueuesService queuesService;

    @Inject
    public QueueStatsAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(QueueStatsRequest request, ActionListener<QueueStatsResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        if (!queuesService.hasQueue(request.getQueue())) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
        }
        TransactionalQueueService queueService = queuesService.queue(request.getQueue());
        try {
            listener.onResponse(new QueueStatsResponse(queueService.stats()));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public int getActionId() {
        return ACTION_ID;
    }

    @Override
    public QueueStatsRequest createRequest() {
        return new QueueStatsRequest();
    }
}
