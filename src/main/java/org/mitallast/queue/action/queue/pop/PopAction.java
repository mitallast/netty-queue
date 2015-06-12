package org.mitallast.queue.action.queue.pop;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class PopAction extends AbstractAction<PopRequest, PopResponse> {

    public final static String actionName = "internal:queue/pop";
    private final TransactionalQueuesService queuesService;

    @Inject
    public PopAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, actionName, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(PopRequest request, Listener<PopResponse> listener) {
        if (!queuesService.hasQueue(request.getQueue())) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
        }
        TransactionalQueueService queueService = queuesService.queue(request.getQueue());
        try {
            QueueMessage message = queueService.lockAndPop();
            listener.onResponse(new PopResponse(message));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public PopRequest createRequest() {
        return new PopRequest();
    }
}