package org.mitallast.queue.action.queue.transactional.commit;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.io.IOException;
import java.util.UUID;

public class TransactionCommitResponse extends ActionResponse {

    public final static ResponseMapper<TransactionCommitResponse> mapper = new ResponseMapper<>(TransactionCommitResponse::new);

    private String queue;
    private UUID transactionUUID;

    public TransactionCommitResponse() {
    }

    public TransactionCommitResponse(String queue, UUID transactionUUID) {
        this.queue = queue;
        this.transactionUUID = transactionUUID;
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

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readText();
        transactionUUID = stream.readUUID();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(queue);
        stream.writeUUID(transactionUUID);
    }
}
