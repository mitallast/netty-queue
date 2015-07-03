package org.mitallast.queue.action.queue.stats;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class QueueStatsRequest implements ActionRequest<QueueStatsRequest> {
    private final String queue;

    private QueueStatsRequest(String queue) {
        this.queue = queue;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue);
    }

    public String queue() {
        return queue;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<QueueStatsRequest> {
        private String queue;

        private Builder from(QueueStatsRequest entry) {
            queue = entry.queue;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        @Override
        public QueueStatsRequest build() {
            return new QueueStatsRequest(queue);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeText(queue);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            queue = stream.readText();
        }
    }
}
