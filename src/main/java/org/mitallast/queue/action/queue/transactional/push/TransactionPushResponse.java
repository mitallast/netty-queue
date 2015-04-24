package org.mitallast.queue.action.queue.transactional.push;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

public class TransactionPushResponse extends ActionResponse {

    private String queue;
    private UUID transactionUUID;
    private UUID messageUUID;

    public TransactionPushResponse() {
    }

    public TransactionPushResponse(String queue, UUID transactionUUID, UUID messageUUID) {
        this.queue = queue;
        this.transactionUUID = transactionUUID;
        this.messageUUID = messageUUID;
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

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readText();
        transactionUUID = stream.readUUID();
        messageUUID = stream.readUUID();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(queue);
        stream.writeUUID(transactionUUID);
        stream.writeUUID(messageUUID);
    }
}
