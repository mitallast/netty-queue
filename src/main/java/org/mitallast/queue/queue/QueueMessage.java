package org.mitallast.queue.queue;

public class QueueMessage<Message> {
    private Message message;

    public QueueMessage() {
    }

    public QueueMessage(Message message) {
        this.message = message;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}
