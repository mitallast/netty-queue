package org.mitallast.queue.action.queues.stats;

import org.mitallast.queue.action.ActionResponse;

import java.util.Set;

public class QueueStatsResponse extends ActionResponse {
    private Set<String> queues;

    public QueueStatsResponse(Set<String> queues) {
        this.queues = queues;
    }

    public Set<String> getQueues() {
        return queues;
    }
}
