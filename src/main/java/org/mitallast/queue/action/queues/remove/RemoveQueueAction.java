package org.mitallast.queue.action.queues.remove;

import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueuesService;

import java.util.concurrent.ExecutorService;

public class RemoveQueueAction extends AbstractAction<RemoveQueueRequest, RemoveQueueResponse> {

    private final QueuesService queuesService;

    public RemoveQueueAction(Settings settings, ExecutorService executorService, QueuesService queuesService) {
        super(settings, executorService);
        this.queuesService = queuesService;
    }

    @Override
    protected void doExecute(RemoveQueueRequest request, ActionListener<RemoveQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        queuesService.removeQueue(request.getQueue(), request.getReason());
        listener.onResponse(new RemoveQueueResponse(true));
    }
}