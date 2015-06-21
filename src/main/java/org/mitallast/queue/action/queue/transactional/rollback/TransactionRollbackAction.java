package org.mitallast.queue.action.queue.transactional.rollback;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class TransactionRollbackAction extends AbstractAction<TransactionRollbackRequest, TransactionRollbackResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public TransactionRollbackAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(TransactionRollbackRequest request, Listener<TransactionRollbackResponse> listener) {
        final TransactionalQueueService queueService = queuesService.queue(request.queue());
        if (queueService == null) {
            listener.onFailure(new QueueMissingException(request.queue()));
            return;
        }
        QueueTransaction transaction = queueService.transaction(request.transactionUUID());
        if (transaction == null) {
            listener.onFailure(new QueueMissingException(request.queue()));
            return;
        }
        try {
            transaction.rollback();
            listener.onResponse(TransactionRollbackResponse.builder()
                .setTransactionUUID(request.transactionUUID())
                .setQueue(request.queue())
                .build());
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }
}
