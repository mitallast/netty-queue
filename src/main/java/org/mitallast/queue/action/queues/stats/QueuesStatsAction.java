package org.mitallast.queue.action.queues.stats;

import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueuesService;

import java.util.concurrent.ExecutorService;

public class QueuesStatsAction extends AbstractAction<QueuesStatsRequest, QueuesStatsResponse> {

    private final QueuesService queuesService;

    public QueuesStatsAction(Settings settings, ExecutorService executorService, QueuesService queuesService) {
        super(settings, executorService);
        this.queuesService = queuesService;
    }

    @Override
    protected void doExecute(QueuesStatsRequest request, ActionListener<QueuesStatsResponse> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        listener.onResponse(new QueuesStatsResponse(queuesService.queues()));
    }
}
