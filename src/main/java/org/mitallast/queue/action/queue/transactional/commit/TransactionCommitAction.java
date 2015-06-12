package org.mitallast.queue.action.queue.transactional.commit;

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

public class TransactionCommitAction extends AbstractAction<TransactionCommitRequest, TransactionCommitResponse> {

    public final static String actionName = "internal:queue/transaction/commit";
    private final TransactionalQueuesService queuesService;

    @Inject
    public TransactionCommitAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, actionName, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(TransactionCommitRequest request, Listener<TransactionCommitResponse> listener) {
        final TransactionalQueueService queueService = queuesService.queue(request.getQueue());
        if (queueService == null) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
            return;
        }
        QueueTransaction transaction = queueService.transaction(request.getTransactionUUID());
        if (transaction == null) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
            return;
        }
        try {
            transaction.commit();
            listener.onResponse(new TransactionCommitResponse(request.getQueue(), request.getTransactionUUID()));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public TransactionCommitRequest createRequest() {
        return new TransactionCommitRequest();
    }
}
