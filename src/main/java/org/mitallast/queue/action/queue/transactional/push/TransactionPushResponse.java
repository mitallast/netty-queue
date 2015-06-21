package org.mitallast.queue.action.queue.transactional.push;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

public class TransactionPushResponse implements ActionResponse<TransactionPushResponse.Builder, TransactionPushResponse> {
    private final UUID transactionUUID;
    private final UUID messageUUID;

    private TransactionPushResponse(UUID transactionUUID, UUID messageUUID) {
        this.transactionUUID = transactionUUID;
        this.messageUUID = messageUUID;
    }

    public UUID transactionUUID() {
        return transactionUUID;
    }

    public UUID messageUUID() {
        return messageUUID;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, TransactionPushResponse> {
        private UUID transactionUUID;
        private UUID messageUUID;

        @Override
        public Builder from(TransactionPushResponse entry) {
            transactionUUID = entry.transactionUUID;
            messageUUID = entry.messageUUID;
            return this;
        }

        public Builder setTransactionUUID(UUID transactionUUID) {
            this.transactionUUID = transactionUUID;
            return this;
        }

        public Builder setMessageUUID(UUID messageUUID) {
            this.messageUUID = messageUUID;
            return this;
        }

        @Override
        public TransactionPushResponse build() {
            return new TransactionPushResponse(transactionUUID, messageUUID);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            transactionUUID = stream.readUUID();
            messageUUID = stream.readUUID();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeUUID(transactionUUID);
            stream.writeUUID(messageUUID);
        }
    }
}
