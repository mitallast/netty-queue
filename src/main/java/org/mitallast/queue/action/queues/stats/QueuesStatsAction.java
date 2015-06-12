package org.mitallast.queue.action.queues.stats;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.stats.QueuesStats;
import org.mitallast.queue.queues.transactional.TransactionalQueuesService;
import org.mitallast.queue.transport.TransportController;

import java.io.IOException;

public class QueuesStatsAction extends AbstractAction<QueuesStatsRequest, QueuesStatsResponse> {

    public final static String actionName = "internal:queues/stats";
    private final TransactionalQueuesService queuesService;

    @Inject
    public QueuesStatsAction(Settings settings, TransportController controller, TransactionalQueuesService queuesService) {
        super(settings, actionName, controller);
        this.queuesService = queuesService;
    }

    @Override
    protected void executeInternal(QueuesStatsRequest request, Listener<QueuesStatsResponse> listener) {
        try {
            QueuesStats stats = queuesService.stats();
            listener.onResponse(new QueuesStatsResponse(stats));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public QueuesStatsRequest createRequest() {
        return new QueuesStatsRequest();
    }
}
