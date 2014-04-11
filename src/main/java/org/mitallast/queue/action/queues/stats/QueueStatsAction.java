package org.mitallast.queue.action.queues.stats;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueuesService;

public class QueueStatsAction extends AbstractAction<QueueStatsRequest, QueueStatsResponse> {

    private final QueuesService queuesService;

    @Inject
    public QueueStatsAction(Settings settings, QueuesService queuesService) {
        super(settings);
        this.queuesService = queuesService;
    }

    @Override
    public void execute(QueueStatsRequest request, ActionListener<QueueStatsResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        listener.onResponse(new QueueStatsResponse(queuesService.queues()));
    }
}
