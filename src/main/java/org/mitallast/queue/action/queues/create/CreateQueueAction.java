package org.mitallast.queue.action.queues.create;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueuesService;

public class CreateQueueAction extends AbstractAction<CreateQueueRequest, CreateQueueResponse> {

    private final QueuesService queuesService;

    @Inject
    public CreateQueueAction(Settings settings, QueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(CreateQueueRequest request, ActionListener<CreateQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        queuesService.createQueue(request.getQueue(), request.getType(), request.getSettings());
        listener.onResponse(new CreateQueueResponse(true));
    }
}
