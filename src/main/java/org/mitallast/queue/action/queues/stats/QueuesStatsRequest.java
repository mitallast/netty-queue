package org.mitallast.queue.action.queues.stats;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class QueuesStatsRequest implements ActionRequest<QueuesStatsRequest> {

    private QueuesStatsRequest() {
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder();
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<QueuesStatsRequest> {

        private Builder from(QueuesStatsRequest entry) {
            return this;
        }

        @Override
        public QueuesStatsRequest build() {
            return new QueuesStatsRequest();
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
        }
    }
}
