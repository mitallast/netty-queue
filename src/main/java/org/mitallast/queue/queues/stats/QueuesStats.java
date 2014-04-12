package org.mitallast.queue.queues.stats;

import java.util.ArrayList;
import java.util.List;

public class QueuesStats {
    private final List<QueueStats> queueStats = new ArrayList<>();

    public void addQueueStats(QueueStats queueStats) {
        this.queueStats.add(queueStats);
    }

    public List<QueueStats> getQueueStats() {
        return queueStats;
    }
}
