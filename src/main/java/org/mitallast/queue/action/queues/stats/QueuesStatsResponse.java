package org.mitallast.queue.action.queues.stats;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.queues.stats.QueuesStats;

public class QueuesStatsResponse extends ActionResponse {
    private QueuesStats stats;

    public QueuesStatsResponse(QueuesStats stats) {
        this.stats = stats;
    }

    public QueuesStats stats() {
        return stats;
    }
}
