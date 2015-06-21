package org.mitallast.queue.action.queues.stats;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queues.stats.QueuesStats;

import java.io.IOException;

public class QueuesStatsResponse implements ActionResponse<QueuesStatsResponse.Builder, QueuesStatsResponse> {

    private final QueuesStats stats;

    private QueuesStatsResponse(QueuesStats stats) {
        this.stats = stats;
    }

    public QueuesStats stats() {
        return stats;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, QueuesStatsResponse> {
        private QueuesStats stats;

        @Override
        public Builder from(QueuesStatsResponse entry) {
            stats = entry.stats;
            return this;
        }

        public Builder setStats(QueuesStats stats) {
            this.stats = stats;
            return this;
        }

        @Override
        public QueuesStatsResponse build() {
            return new QueuesStatsResponse(stats);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            stats = stream.readStreamable(QueuesStats::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeStreamable(stats);
        }
    }
}
