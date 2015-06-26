package org.mitallast.queue.action.queues.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class DeleteQueueAction extends AbstractAction<DeleteQueueRequest, DeleteQueueResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public DeleteQueueAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(DeleteQueueRequest request, CompletableFuture<DeleteQueueResponse> listener) {
        try {
            queuesService.deleteQueue(request.queue(), request.reason());
            listener.complete(DeleteQueueResponse.builder().setDeleted(true).build());
        } catch (QueueMissingException e) {
            listener.complete(DeleteQueueResponse.builder().setDeleted(false).setError(e).build());
        } catch (IOException e) {
            listener.completeExceptionally(e);
        }
    }
}