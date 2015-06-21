package org.mitallast.queue.action.queue.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMessageNotFoundException;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class DeleteAction extends AbstractAction<DeleteRequest, DeleteResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public DeleteAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(DeleteRequest request, Listener<DeleteResponse> listener) {
        if (!queuesService.hasQueue(request.queue())) {
            listener.onFailure(new QueueMissingException(request.queue()));
        }
        TransactionalQueueService queueService = queuesService.queue(request.queue());
        try {
            QueueMessage message = queueService.unlockAndDelete(request.messageUUID());
            if (message != null) {
                listener.onResponse(DeleteResponse.builder().setMessage(message).build());
            } else {
                listener.onFailure(new QueueMessageNotFoundException(request.messageUUID()));
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }
}
