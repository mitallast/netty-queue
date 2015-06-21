package org.mitallast.queue.action.queues.create;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class CreateQueueAction extends AbstractAction<CreateQueueRequest, CreateQueueResponse> {

    private final TransactionalQueuesService queuesService;

    @Inject
    public CreateQueueAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(CreateQueueRequest request, Listener<CreateQueueResponse> listener) {
        try {
            queuesService.createQueue(request.queue(), request.settings());
            listener.onResponse(CreateQueueResponse.builder().build());
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }
}
