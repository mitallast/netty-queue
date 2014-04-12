package org.mitallast.queue.queue;

public class QueueMessage<Message> {
    private long index;
    private Message message;

    public QueueMessage() {
    }

    public QueueMessage(Message message) {
        this.message = message;
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
