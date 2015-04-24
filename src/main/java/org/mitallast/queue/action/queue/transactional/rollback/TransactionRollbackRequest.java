package org.mitallast.queue.action.queue.transactional.rollback;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.strings.Strings;

import java.io.IOException;
import java.util.UUID;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class TransactionRollbackRequest extends ActionRequest {

    private String queue;
    private UUID transactionUuid;

    @Override
    public ActionType actionType() {
        return ActionType.TRANSACTION_ROLLBACK;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(queue)) {
            validationException = addValidationError("queue is missing", null);
        }
        if (transactionUuid == null) {
            validationException = addValidationError("transactionUuid is missing", null);
        }
        return validationException;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public UUID getTransactionUuid() {
        return transactionUuid;
    }

    public void setTransactionUuid(UUID transactionUuid) {
        this.transactionUuid = transactionUuid;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readTextOrNull();
        transactionUuid = stream.readUUIDOrNull();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue);
        stream.writeUUIDOrNull(transactionUuid);
    }
}
