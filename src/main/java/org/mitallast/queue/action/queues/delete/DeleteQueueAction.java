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

    private final TransactionalQueuesService queuesService;

    @Inject
    public DeleteQueueAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(DeleteQueueRequest request, Listener<DeleteQueueResponse> listener) {
        try {
            queuesService.deleteQueue(request.queue(), request.reason());
            listener.onResponse(DeleteQueueResponse.builder().setDeleted(true).build());
        } catch (QueueMissingException e) {
            listener.onResponse(DeleteQueueResponse.builder().setDeleted(false).setError(e).build());
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }
}