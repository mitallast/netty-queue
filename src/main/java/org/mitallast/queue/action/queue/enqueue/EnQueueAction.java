package org.mitallast.queue.action.queue.enqueue;

import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueTypeMismatch;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.QueuesService;

import java.util.concurrent.ExecutorService;

public class EnQueueAction extends AbstractAction<EnQueueRequest, EnQueueResponse> {

    private final QueuesService queuesService;

    public EnQueueAction(Settings settings, ExecutorService executorService, QueuesService queuesService) {
        super(settings, executorService);
        this.queuesService = queuesService;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doExecute(EnQueueRequest request, ActionListener<EnQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        if (!queuesService.hasQueue(request.getQueue())) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
        }
        QueueService queueService = queuesService.queue(request.getQueue());
        if (!queueService.isSupported(request.getMessage())) {
            listener.onFailure(new QueueTypeMismatch());
        }
        queueService.enqueue(request.getMessage());
        listener.onResponse(new EnQueueResponse(true));
    }
}