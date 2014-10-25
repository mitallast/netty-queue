package org.mitallast.queue.action.queue.delete;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.QueuesService;

public class DeleteAction extends AbstractAction<DeleteRequest, DeleteResponse> {

    private final QueuesService queuesService;

    @Inject
    public DeleteAction(Settings settings, QueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        if (!queuesService.hasQueue(request.getQueue())) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
        }
        QueueService queueService = queuesService.queue(request.getQueue());
        queueService.delete(request.getUuid());
        listener.onResponse(new DeleteResponse(true));
    }
}
