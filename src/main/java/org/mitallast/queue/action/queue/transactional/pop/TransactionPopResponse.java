package org.mitallast.queue.action.queue.transactional.pop;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;
import java.util.UUID;

public class TransactionPopResponse implements ActionResponse<TransactionPopResponse.Builder, TransactionPopResponse> {
    private final UUID transactionUUID;
    private final QueueMessage message;

    private TransactionPopResponse(UUID transactionUUID, QueueMessage message) {
        this.transactionUUID = transactionUUID;
        this.message = message;
    }

    public UUID transactionUUID() {
        return transactionUUID;
    }

    public QueueMessage message() {
        return message;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, TransactionPopResponse> {
        private UUID transactionUUID;
        private QueueMessage message;

        @Override
        public Builder from(TransactionPopResponse entry) {
            transactionUUID = entry.transactionUUID;
            message = entry.message;
            return this;
        }

        public Builder setTransactionUUID(UUID transactionUUID) {
            this.transactionUUID = transactionUUID;
            return this;
        }

        public Builder setMessage(QueueMessage message) {
            this.message = message;
            return this;
        }

        @Override
        public TransactionPopResponse build() {
            return new TransactionPopResponse(transactionUUID, message);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            transactionUUID = stream.readUUID();
            message = stream.readStreamable(QueueMessage::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeUUID(transactionUUID);
            stream.writeStreamable(message);
        }
    }
}
