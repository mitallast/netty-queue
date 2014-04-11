package org.mitallast.queue.action.queues.remove;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueuesService;

public class RemoveQueueAction extends AbstractAction<RemoveQueueRequest, RemoveQueueResponse> {

    private final QueuesService queuesService;

    @Inject
    public RemoveQueueAction(Settings settings, QueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(RemoveQueueRequest request, ActionListener<RemoveQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        queuesService.deleteQueue(request.getQueue(), request.getReason());
        listener.onResponse(new RemoveQueueResponse(true));
    }
}