package org.mitallast.queue.action.queue.transactional.delete;

import org.mitallast.queue.action.queue.delete.DeleteResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.io.IOException;
import java.util.UUID;

public class TransactionDeleteResponse extends DeleteResponse {

    public final static ResponseMapper<TransactionDeleteResponse> mapper = new ResponseMapper<>(TransactionDeleteResponse::new);

    private UUID transactionUUID;
    private UUID messageUUID;

    public TransactionDeleteResponse() {
    }

    public TransactionDeleteResponse(UUID transactionUUID, UUID messageUUID, QueueMessage deleted) {
        super(deleted);
        this.transactionUUID = transactionUUID;
        this.messageUUID = messageUUID;
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
        super.readFrom(stream);
        transactionUUID = stream.readUUID();
        messageUUID = stream.readUUID();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        super.writeTo(stream);
        stream.writeUUID(transactionUUID);
        stream.writeUUID(messageUUID);
    }
}
