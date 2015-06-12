package org.mitallast.queue.action.queues.create;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class CreateQueueRequest extends ActionRequest {
    private String queue;
    private Settings settings = ImmutableSettings.EMPTY;

    public CreateQueueRequest() {
    }

    public CreateQueueRequest(String queue) {
        this.queue = queue;
    }

    public CreateQueueRequest(String queue, Settings settings) {
        this.queue = queue;
        this.settings = settings;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Settings getSettings() {
        return settings;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue)
            .missing("settings", settings);
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readTextOrNull();
        settings = stream.readSettings();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue);
        stream.writeSettings(settings);
    }
}
