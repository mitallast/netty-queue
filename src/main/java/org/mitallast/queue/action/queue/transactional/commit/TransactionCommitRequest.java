package org.mitallast.queue.action.queue.transactional.commit;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.strings.Strings;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;
import java.util.UUID;

public class TransactionCommitRequest extends ActionRequest {

    private String queue;
    private UUID transactionUUID;

    @Override
    public ActionType actionType() {
        return ActionType.TRANSACTION_COMMIT;
    }

    @Override
    public ValidationBuilder validate() {
        ValidationBuilder builder = ValidationBuilder.builder();
        if (Strings.isEmpty(queue)) {
            builder = builder.missing("queue");
        }
        if (transactionUUID == null) {
            builder = builder.missing("transactionUUID");
        }
        return builder;
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
