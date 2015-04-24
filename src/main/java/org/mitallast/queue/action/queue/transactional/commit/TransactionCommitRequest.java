package org.mitallast.queue.action.queue.transactional.commit;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.strings.Strings;

import java.io.IOException;
import java.util.UUID;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class TransactionCommitRequest extends ActionRequest {

    private String queue;
    private UUID transactionUUID;

    @Override
    public ActionType actionType() {
        return ActionType.TRANSACTION_COMMIT;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(queue)) {
            validationException = addValidationError("queue is missing", null);
        }
        if (transactionUUID == null) {
            validationException = addValidationError("transactionUUID is missing", null);
        }
        return validationException;
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
