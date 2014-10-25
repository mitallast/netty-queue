package org.mitallast.queue.action.queue.peek;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.QueuesService;

public class PeekQueueAction extends AbstractAction<PeekQueueRequest, PeekQueueResponse> {

    private final QueuesService queuesService;

    @Inject
    public PeekQueueAction(Settings settings, QueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(PeekQueueRequest request, ActionListener<PeekQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        if (!queuesService.hasQueue(request.getQueue())) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
        }
        QueueService queueService = queuesService.queue(request.getQueue());
        QueueMessage message = queueService.peek();
        listener.onResponse(new PeekQueueResponse(message));
    }
}