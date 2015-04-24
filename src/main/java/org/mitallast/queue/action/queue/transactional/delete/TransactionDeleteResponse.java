package org.mitallast.queue.action.queue.transactional.delete;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;
import java.util.UUID;

public class TransactionDeleteResponse extends ActionResponse {

    private String queue;
    private UUID transactionUUID;
    private UUID messageUUID;
    private QueueMessage deleted;

    public TransactionDeleteResponse() {
    }

    public TransactionDeleteResponse(String queue, UUID transactionUUID, UUID messageUUID, QueueMessage deleted) {
        this.queue = queue;
        this.transactionUUID = transactionUUID;
        this.messageUUID = messageUUID;
        this.deleted = deleted;
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

    public UUID getMessageUUID() {
        return messageUUID;
    }

    public void setMessageUUID(UUID messageUUID) {
        this.messageUUID = messageUUID;
    }

    public QueueMessage getDeleted() {
        return deleted;
    }

    public void setDeleted(QueueMessage deleted) {
        this.deleted = deleted;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readText();
        transactionUUID = stream.readUUID();
        messageUUID = stream.readUUID();
        if (stream.readBoolean()) {
            deleted = new QueueMessage();
            deleted.readFrom(stream);
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(queue);
        stream.writeUUID(transactionUUID);
        stream.writeUUID(messageUUID);
        if (deleted != null) {
            stream.writeBoolean(true);
            deleted.writeTo(stream);
        } else {
            stream.writeBoolean(false);
        }
    }
}
