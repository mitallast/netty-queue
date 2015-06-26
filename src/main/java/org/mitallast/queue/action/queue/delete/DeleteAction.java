package org.mitallast.queue.action.queue.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMessageNotFoundException;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class DeleteAction extends AbstractAction<DeleteRequest, DeleteResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public DeleteAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(DeleteRequest request, CompletableFuture<DeleteResponse> listener) {
        if (!queuesService.hasQueue(request.queue())) {
            listener.completeExceptionally(new QueueMissingException(request.queue()));
            return;
        }
        TransactionalQueueService queueService = queuesService.queue(request.queue());
        try {
            QueueMessage message = queueService.unlockAndDelete(request.messageUUID());
            if (message != null) {
                listener.complete(DeleteResponse.builder().setMessage(message).build());
            } else {
                listener.completeExceptionally(new QueueMessageNotFoundException(request.messageUUID()));
            }
        } catch (IOException e) {
            listener.completeExceptionally(e);
        }
    }
}
