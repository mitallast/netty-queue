package org.mitallast.queue.action.queues.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueuesService;

public class DeleteQueueAction extends AbstractAction<DeleteQueueRequest, DeleteQueueResponse> {

    private final QueuesService queuesService;

    @Inject
    public DeleteQueueAction(Settings settings, QueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(DeleteQueueRequest request, ActionListener<DeleteQueueResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        queuesService.deleteQueue(request.getQueue(), request.getReason());
        listener.onResponse(new DeleteQueueResponse());
    }
}