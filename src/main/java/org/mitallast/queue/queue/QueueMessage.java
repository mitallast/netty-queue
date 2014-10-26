package org.mitallast.queue.queue;

import java.nio.charset.Charset;
import java.util.UUID;

public class QueueMessage {

    public static final Charset defaultCharset = Charset.forName("UTF-8");

    public UUID uuid;
    private String message;
    private Charset charset = defaultCharset;

    public QueueMessage() {
    }

    public QueueMessage(String message, UUID uuid) {
        setUuid(uuid);
        setMessage(message);
    }

    public QueueMessage(UUID uuid, byte[] message) {
        setUuid(uuid);
        setMessage(message);
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

    public byte[] getMessageBytes() {
        return message.getBytes(charset);
    }

    public void setMessage(byte[] bytes) {
        message = new String(bytes, charset);
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
