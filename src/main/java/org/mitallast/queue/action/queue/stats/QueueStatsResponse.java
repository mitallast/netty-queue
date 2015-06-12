package org.mitallast.queue.action.queue.stats;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queues.stats.QueueStats;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.io.IOException;

public class QueueStatsResponse extends ActionResponse {

    public final static ResponseMapper<QueueStatsResponse> mapper = new ResponseMapper<>(QueueStatsResponse::new);

    private QueueStats stats;

    public QueueStatsResponse() {
    }

    public QueueStatsResponse(QueueStats stats) {
        this.stats = stats;
    }

    public QueueStats getStats() {
        return stats;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        stats = stream.readStreamableOrNull(QueueStats::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamableOrNull(stats);
    }
}