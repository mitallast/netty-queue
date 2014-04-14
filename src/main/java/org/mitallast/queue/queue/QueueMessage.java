package org.mitallast.queue.queue;

public class QueueMessage<Message> {
    public String uid;
    private long index;
    private Message message;

    public QueueMessage() {
    }

    public QueueMessage(Message message) {
        this.message = message;
    }

    public QueueMessage(Message message, String uid) {
        this.message = message;
        this.uid = uid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}
