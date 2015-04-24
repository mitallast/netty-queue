package org.mitallast.queue.action.queue.transactional.pop;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.transport.TransportController;

import java.io.IOException;

public class TransactionPopAction extends AbstractAction<TransactionPopRequest, TransactionPopResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public TransactionPopAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(TransactionPopRequest request, Listener<TransactionPopResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
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
            QueueMessage message = transaction.pop();
            listener.onResponse(new TransactionPopResponse(request.getQueue(), request.getTransactionUUID(), message));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public ActionType getActionId() {
        return ActionType.TRANSACTION_POP;
    }

    @Override
    public TransactionPopRequest createRequest() {
        return new TransactionPopRequest();
    }
}
