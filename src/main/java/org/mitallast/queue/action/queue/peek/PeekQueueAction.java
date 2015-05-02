package org.mitallast.queue.action.queue.peek;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class PeekQueueAction extends AbstractAction<PeekQueueRequest, PeekQueueResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public PeekQueueAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(PeekQueueRequest request, Listener<PeekQueueResponse> listener) {
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
            QueueMessage message = queueService.peek();
            listener.onResponse(new PeekQueueResponse(message));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public ActionType getActionId() {
        return ActionType.QUEUE_PEEK;
    }

    @Override
    public PeekQueueRequest createRequest() {
        return new PeekQueueRequest();
    }
}