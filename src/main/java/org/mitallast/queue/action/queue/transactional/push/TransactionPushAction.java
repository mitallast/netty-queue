package org.mitallast.queue.action.queue.transactional.push;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class TransactionPushAction extends AbstractAction<TransactionPushRequest, TransactionPushResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public TransactionPushAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(TransactionPushRequest request, Listener<TransactionPushResponse> listener) {
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
            transaction.push(request.getMessage());
            listener.onResponse(new TransactionPushResponse(
                request.getTransactionUUID(),
                request.getMessage().getUuid()
            ));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public ActionType getActionId() {
        return ActionType.TRANSACTION_PUSH;
    }

    @Override
    public TransactionPushRequest createRequest() {
        return new TransactionPushRequest();
    }
}
