package org.mitallast.queue.action.queue.transactional.commit;

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

public class TransactionCommitAction extends AbstractAction<TransactionCommitRequest, TransactionCommitResponse> {
    private final TransactionalQueuesService queuesService;

    @Inject
    public TransactionCommitAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(TransactionCommitRequest request, CompletableFuture<TransactionCommitResponse> listener) {
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
            transaction.commit();
            listener.complete(TransactionCommitResponse.builder()
                .setQueue(request.queue())
                .setTransactionUUID(request.transactionUUID())
                .build());
        } catch (IOException e) {
            listener.completeExceptionally(e);
        }
    }
}
