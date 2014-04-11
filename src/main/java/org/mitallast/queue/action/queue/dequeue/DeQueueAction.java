package org.mitallast.queue.action.queue.dequeue;

import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.QueuesService;

import java.util.concurrent.ExecutorService;

public class DeQueueAction extends AbstractAction<DeQueueRequest, DeQueueResponse> {

    private final QueuesService queuesService;

    public DeQueueAction(Settings settings, ExecutorService executorService, QueuesService queuesService) {
        super(settings, executorService);
        this.queuesService = queuesService;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doExecute(DeQueueRequest request, ActionListener<DeQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        if (!queuesService.hasQueue(request.getQueue())) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
        }
        QueueService queueService = queuesService.queue(request.getQueue());
        QueueMessage message = queueService.dequeue();
        listener.onResponse(new DeQueueResponse(message));
    }
}