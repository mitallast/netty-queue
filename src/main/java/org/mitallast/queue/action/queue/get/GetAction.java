package org.mitallast.queue.action.queue.get;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class GetAction extends AbstractAction<GetRequest, GetResponse> {

    public final static int ACTION_ID = 4;
    private final TransactionalQueuesService queuesService;

    @Inject
    public GetAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(GetRequest request, ActionListener<GetResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        if (!queuesService.hasQueue(request.getQueue())) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
        }
        TransactionalQueueService queueService = queuesService.queue(request.getQueue());
        try {
            QueueMessage message = queueService.get(request.getUuid());
            listener.onResponse(new GetResponse(message));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public int getActionId() {
        return ACTION_ID;
    }

    @Override
    public GetRequest createRequest() {
        return new GetRequest();
    }
}
