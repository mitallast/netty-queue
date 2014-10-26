package org.mitallast.queue.queue;

import java.util.UUID;

public class QueueMessage {
    public UUID uuid;
    private String message;

    public QueueMessage() {
    }

    public QueueMessage(String message, UUID uuid) {
        this.message = message;
        this.uuid = uuid;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        setUuid(UUID.fromString(uuid));
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueMessage that = (QueueMessage) o;

        if (message != null ? !message.equals(that.message) : that.message != null) return false;
        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }
}
