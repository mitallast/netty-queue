package org.mitallast.queue.action.queues.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class DeleteQueueAction extends AbstractAction<DeleteQueueRequest, DeleteQueueResponse> {

    public final static String actionName = "internal:queues/delete";
    private final TransactionalQueuesService queuesService;

    @Inject
    public DeleteQueueAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, actionName, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(DeleteQueueRequest request, Listener<DeleteQueueResponse> listener) {
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
    public DeleteQueueRequest createRequest() {
        return new DeleteQueueRequest();
    }
}