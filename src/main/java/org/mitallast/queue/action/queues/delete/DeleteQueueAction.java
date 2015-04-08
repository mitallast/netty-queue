package org.mitallast.queue.action.queues.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class DeleteQueueAction extends AbstractAction<DeleteQueueRequest, DeleteQueueResponse> {

    public final static int ACTION_ID = 8;
    private final TransactionalQueuesService queuesService;

    @Inject
    public DeleteQueueAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
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

    @Override
    public int getActionId() {
        return ACTION_ID;
    }

    @Override
    public DeleteQueueRequest createRequest() {
        return new DeleteQueueRequest();
    }
}