package org.mitallast.queue.action.queues.create;

import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueuesService;

import java.util.concurrent.ExecutorService;

public class CreateQueueAction extends AbstractAction<CreateQueueRequest, CreateQueueResponse> {

    private final QueuesService queuesService;

    public CreateQueueAction(Settings settings, ExecutorService executorService, QueuesService queuesService) {
        super(settings, executorService);
        this.queuesService = queuesService;
    }

    @Override
    protected void doExecute(CreateQueueRequest request, ActionListener<CreateQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        queuesService.createQueue(request.getQueue(), request.getType(), request.getSettings());
        listener.onResponse(new CreateQueueResponse(true));
    }
}
