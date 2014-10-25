package org.mitallast.queue.queue;

import java.util.UUID;

public class QueueMessage<Message> {
    public UUID uid;
    private Message message;

    public QueueMessage() {
    }

    public QueueMessage(Message message, UUID uid) {
        this.message = message;
        this.uid = uid;
    }

    public UUID getUid() {
        return uid;
    }

    public void setUid(String uid) {
        setUid(UUID.fromString(uid));
    }

    public void setUid(UUID uid) {
        this.uid = uid;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueMessage that = (QueueMessage) o;

        if (message != null ? !message.equals(that.message) : that.message != null) return false;
        if (uid != null ? !uid.equals(that.uid) : that.uid != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = uid != null ? uid.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }
}
