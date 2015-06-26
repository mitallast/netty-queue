package org.mitallast.queue.action.queue.transactional.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class TransactionDeleteAction extends AbstractAction<TransactionDeleteRequest, TransactionDeleteResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public TransactionDeleteAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(TransactionDeleteRequest request, CompletableFuture<TransactionDeleteResponse> listener) {
        final TransactionalQueueService queueService = queuesService.queue(request.queue());
        if (queueService == null) {
            listener.completeExceptionally(new QueueMissingException(request.queue()));
            return;
        }
        QueueTransaction transaction = queueService.transaction(request.transactionUUID());
        if (transaction == null) {
            listener.completeExceptionally(new QueueMissingException(request.queue()));
            return;
        }
        try {
            QueueMessage deleted = transaction.delete(request.messageUUID());
            listener.complete(TransactionDeleteResponse.builder().
                setTransactionUUID(request.transactionUUID())
                .setMessageUUID(request.messageUUID())
                .setDeleted(deleted)
                .build());
        } catch (IOException e) {
            listener.completeExceptionally(e);
        }
    }
}
