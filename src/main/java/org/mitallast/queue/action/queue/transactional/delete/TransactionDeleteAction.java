package org.mitallast.queue.action.queue.transactional.delete;

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

public class TransactionDeleteAction extends AbstractAction<TransactionDeleteRequest, TransactionDeleteResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public TransactionDeleteAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(TransactionDeleteRequest request, Listener<TransactionDeleteResponse> listener) {
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
            QueueMessage deleted = transaction.delete(request.getMessageUUID());
            listener.onResponse(new TransactionDeleteResponse(
                request.getQueue(),
                request.getTransactionUUID(),
                request.getMessageUUID(),
                deleted
            ));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public ActionType getActionId() {
        return ActionType.TRANSACTION_DELETE;
    }

    @Override
    public TransactionDeleteRequest createRequest() {
        return new TransactionDeleteRequest();
    }
}
