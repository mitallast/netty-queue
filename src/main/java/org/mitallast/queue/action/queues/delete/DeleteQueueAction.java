package org.mitallast.queue.action.queues.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;

import java.io.IOException;

public class DeleteQueueAction extends AbstractAction<DeleteQueueRequest, DeleteQueueResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public DeleteQueueAction(Settings settings, TransactionalQueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(DeleteQueueRequest request, ActionListener<DeleteQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        try {
            queuesService.deleteQueue(request.getQueue(), request.getReason());
            listener.onResponse(new DeleteQueueResponse(true));
        } catch (QueueMissingException e) {
            listener.onResponse(new DeleteQueueResponse(false, e));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }
}