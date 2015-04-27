package org.mitallast.queue.action.queue.transactional.pop;

import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.action.queue.pop.PopRequest;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class TransactionPopRequest extends PopRequest {

    private UUID transactionUUID;

    @Override
    public ActionType actionType() {
        return ActionType.TRANSACTION_POP;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (transactionUUID == null) {
            validationException = addValidationError("transactionUUID is missing", validationException);
        }
        return validationException;
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
