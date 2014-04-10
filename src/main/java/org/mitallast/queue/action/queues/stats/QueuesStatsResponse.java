package org.mitallast.queue.action.queues.stats;

import org.mitallast.queue.action.ActionResponse;

import java.util.Set;

public class QueuesStatsResponse extends ActionResponse {
    private Set<String> queues;

    public QueuesStatsResponse(Set<String> queues) {
        this.queues = queues;
    }

    public Set<String> getQueues() {
        return queues;
    }
}
