package org.mitallast.queue.action.queues.create;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class CreateQueueRequest implements ActionRequest<CreateQueueRequest> {
    private final String queue;
    private final Settings settings;

    private CreateQueueRequest(String queue, Settings settings) {
        this.queue = queue;
        this.settings = settings;
    }

    public String queue() {
        return queue;
    }

    public Settings settings() {
        return settings;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue)
            .missing("settings", settings);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<CreateQueueRequest> {
        private String queue;
        private Settings settings = ImmutableSettings.EMPTY;

        private Builder from(CreateQueueRequest entry) {
            queue = entry.queue;
            settings = entry.settings;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        @Override
        public CreateQueueRequest build() {
            return new CreateQueueRequest(queue, settings);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            queue = stream.readText();
            settings = stream.readSettings();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeText(queue);
            stream.writeSettings(settings);
        }
    }
}
