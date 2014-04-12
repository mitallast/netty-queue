package org.mitallast.queue.action.queue.stats;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.queues.stats.QueueStats;

public class QueueStatsResponse extends ActionResponse {
    private QueueStats stats;

    public QueueStatsResponse(QueueStats stats) {
        this.stats = stats;
    }

    public QueueStats getStats() {
        return stats;
    }
}