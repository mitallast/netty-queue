package org.mitallast.queue.action.queue.push;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class PushAction extends AbstractAction<PushRequest, PushResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public PushAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(PushRequest request, Listener<PushResponse> listener) {
        TransactionalQueueService queueService = queuesService.queue(request.getQueue());
        if (queueService == null) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
            return;
        }
        try {
            queueService.push(request.getMessage());
            listener.onResponse(new PushResponse(request.getMessage().getUuid()));
        } catch (QueueMessageUuidDuplicateException | IOException e) {
            listener.onFailure(e);
        }
    }
}