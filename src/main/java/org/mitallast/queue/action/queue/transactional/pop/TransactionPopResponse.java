package org.mitallast.queue.action.queue.transactional.pop;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;
import java.util.UUID;

public class TransactionPopResponse extends ActionResponse {

    private String queue;
    private UUID transactionUUID;
    private QueueMessage message;

    public TransactionPopResponse() {
    }

    public TransactionPopResponse(String queue, UUID transactionUUID, QueueMessage message) {
        this.queue = queue;
        this.transactionUUID = transactionUUID;
        this.message = message;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public UUID getTransactionUUID() {
        return transactionUUID;
    }

    public void setTransactionUUID(UUID transactionUUID) {
        this.transactionUUID = transactionUUID;
    }

    public QueueMessage getMessage() {
        return message;
    }

    public void setMessage(QueueMessage message) {
        this.message = message;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readText();
        transactionUUID = stream.readUUID();
        if (stream.readBoolean()) {
            message = new QueueMessage();
            message.readFrom(stream);
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(queue);
        stream.writeUUID(transactionUUID);
        if (message != null) {
            stream.writeBoolean(true);
            message.writeTo(stream);
        } else {
            stream.writeBoolean(false);
        }
    }
}
