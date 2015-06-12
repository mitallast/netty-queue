package org.mitallast.queue.action.queue.transactional.pop;

import org.mitallast.queue.action.queue.pop.PopResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.io.IOException;
import java.util.UUID;

public class TransactionPopResponse extends PopResponse {

    public final static ResponseMapper<TransactionPopResponse> mapper = new ResponseMapper<>(TransactionPopResponse::new);

    private UUID transactionUUID;

    public TransactionPopResponse() {
    }

    public TransactionPopResponse(UUID transactionUUID, QueueMessage message) {
        super(message);
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
