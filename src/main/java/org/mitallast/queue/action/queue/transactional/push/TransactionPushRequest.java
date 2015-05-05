package org.mitallast.queue.action.queue.transactional.push;

import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.action.queue.push.PushRequest;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;
import java.util.UUID;

public class TransactionPushRequest extends PushRequest {

    private UUID transactionUUID;

    @Override
    public ActionType actionType() {
        return ActionType.TRANSACTION_PUSH;
    }

    @Override
    public ValidationBuilder validate() {
        ValidationBuilder builder = super.validate();
        if (transactionUUID == null) {
            builder = builder.missing("transactionUUID");
        }
        return builder;
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
