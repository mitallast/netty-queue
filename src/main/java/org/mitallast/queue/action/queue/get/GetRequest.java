package org.mitallast.queue.action.queue.get;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;
import java.util.UUID;

public class GetRequest extends ActionRequest {

    private String queue;
    private UUID uuid;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue)
            .missing("uuid", uuid);
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readTextOrNull();
        uuid = stream.readUUIDOrNull();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue);
        stream.writeUUIDOrNull(uuid);
    }
}