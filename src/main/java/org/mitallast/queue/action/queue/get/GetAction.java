package org.mitallast.queue.action.queue.get;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.queues.QueuesService;

public class GetAction extends AbstractAction<GetRequest, GetResponse> {

    private final QueuesService queuesService;

    @Inject
    public GetAction(Settings settings, QueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(GetRequest request, ActionListener<GetResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        if (!queuesService.hasQueue(request.getQueue())) {
            listener.onFailure(new QueueMissingException(request.getQueue()));
        }
        QueueService queueService = queuesService.queue(request.getQueue());
        QueueMessage message = queueService.get(request.getUuid());
        listener.onResponse(new GetResponse(message));
    }
}
