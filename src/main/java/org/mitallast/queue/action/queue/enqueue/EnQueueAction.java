package org.mitallast.queue.action.queue.enqueue;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class EnQueueAction extends AbstractAction<EnQueueRequest, EnQueueResponse> {

    public final static int ACTION_ID = 1;
    private final TransactionalQueuesService queuesService;

    @Inject
    public EnQueueAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(EnQueueRequest request, ActionListener<EnQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        TransactionalQueueService queueService = queuesService.queue(request.getQueue());
        if (queueService == null) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
            return;
        }
        try {
            queueService.push(request.getMessage());
            listener.onResponse(new EnQueueResponse(request.getMessage().getUuid()));
        } catch (QueueMessageUuidDuplicateException | IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public EnQueueRequest createRequest() {
        return new EnQueueRequest();
    }

    @Override
    public int getActionId() {
        return ACTION_ID;
    }
}