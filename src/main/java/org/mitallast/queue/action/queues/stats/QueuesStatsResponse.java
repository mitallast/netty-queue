package org.mitallast.queue.action.queues.stats;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queues.stats.QueuesStats;

import java.io.IOException;

public class QueuesStatsResponse extends ActionResponse {
    private QueuesStats stats;

    public QueuesStatsResponse() {
    }

    public QueuesStatsResponse(QueuesStats stats) {
        this.stats = stats;
    }

    public QueuesStats stats() {
        return stats;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        if (stream.readBoolean()) {
            stats = new QueuesStats();
            stats.readFrom(stream);
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        if (stats == null) {
            stream.writeBoolean(false);
        } else {
            stream.writeBoolean(true);
            stats = new QueuesStats();
            stats.writeTo(stream);
        }
    }
}
