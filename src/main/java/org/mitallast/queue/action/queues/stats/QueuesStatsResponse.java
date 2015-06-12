package org.mitallast.queue.action.queues.stats;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queues.stats.QueuesStats;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.io.IOException;

public class QueuesStatsResponse extends ActionResponse {

    public final static ResponseMapper<QueuesStatsResponse> mapper = new ResponseMapper<>(QueuesStatsResponse::new);

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
        stats = stream.readStreamableOrNull(QueuesStats::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamableOrNull(stats);
    }
}
