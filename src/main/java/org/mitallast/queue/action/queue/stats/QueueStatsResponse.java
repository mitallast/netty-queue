package org.mitallast.queue.action.queue.stats;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queues.stats.QueueStats;

import java.io.IOException;

public class QueueStatsResponse implements ActionResponse<QueueStatsResponse.Builder, QueueStatsResponse> {
    private final QueueStats stats;

    private QueueStatsResponse(QueueStats stats) {
        this.stats = stats;
    }

    public QueueStats stats() {
        return stats;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, QueueStatsResponse> {
        private QueueStats stats;

        @Override
        public Builder from(QueueStatsResponse entry) {
            stats = entry.stats;
            return this;
        }

        public Builder setStats(QueueStats stats) {
            this.stats = stats;
            return this;
        }

        @Override
        public QueueStatsResponse build() {
            return new QueueStatsResponse(stats);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            stats = stream.readStreamable(QueueStats::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeStreamable(stats);
        }
    }
}