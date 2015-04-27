package org.mitallast.queue.action.queue.transactional.push;

import org.mitallast.queue.action.queue.push.PushResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

public class TransactionPushResponse extends PushResponse {

    private UUID transactionUUID;

    public TransactionPushResponse() {
    }

    public TransactionPushResponse(UUID transactionUUID, UUID messageUUID) {
        super(messageUUID);
        this.transactionUUID = transactionUUID;
    }

    public UUID getTransactionUUID() {
        return transactionUUID;
    }

    public void setTransactionUUID(UUID transactionUUID) {
        this.transactionUUID = transactionUUID;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        super.readFrom(stream);
        transactionUUID = stream.readUUID();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        super.writeTo(stream);
        stream.writeUUID(transactionUUID);
    }
}
