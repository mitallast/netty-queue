package org.mitallast.queue.action.queue.transactional.push;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class TransactionPushAction extends AbstractAction<TransactionPushRequest, TransactionPushResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public TransactionPushAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(TransactionPushRequest request, CompletableFuture<TransactionPushResponse> listener) {
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
            transaction.push(request.message());
            listener.complete(TransactionPushResponse.builder()
                .setTransactionUUID(request.transactionUUID())
                .setMessageUUID(request.message().getUuid())
                .build());
        } catch (IOException e) {
            listener.completeExceptionally(e);
        }
    }
}
