package org.mitallast.queue.action.queue.enqueue;

import org.mitallast.queue.action.ActionResponse;

import java.util.UUID;

public class EnQueueResponse extends ActionResponse {

    private final UUID uuid;

    public EnQueueResponse(UUID uuid) {
        this.uuid = uuid;
    }

    public UUID getUUID() {
        return uuid;
    }
}
