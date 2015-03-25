package org.mitallast.queue.action.queue.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMessageNotFoundException;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;

import java.io.IOException;

public class DeleteAction extends AbstractAction<DeleteRequest, DeleteResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public DeleteAction(Settings settings, TransactionalQueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(DeleteRequest request, ActionListener<DeleteResponse> listener) {
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
            QueueMessage message = queueService.unlockAndDelete(request.getUuid());
            if (message != null) {
                listener.onResponse(new DeleteResponse(message));
            } else {
                listener.onFailure(new QueueMessageNotFoundException(request.getUuid()));
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }
}
