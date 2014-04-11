package org.mitallast.queue.action.queue.dequeue;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.QueuesService;

public class DeQueueAction extends AbstractAction<DeQueueRequest, DeQueueResponse> {

    private final QueuesService queuesService;

    @Inject
    public DeQueueAction(Settings settings, QueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(DeQueueRequest request, ActionListener<DeQueueResponse> listener) {
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