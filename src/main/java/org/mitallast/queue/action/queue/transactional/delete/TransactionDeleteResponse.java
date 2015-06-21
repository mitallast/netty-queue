package org.mitallast.queue.action.queue.transactional.delete;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;
import java.util.UUID;

public class TransactionDeleteResponse implements ActionResponse<TransactionDeleteResponse.Builder, TransactionDeleteResponse> {
    private final UUID transactionUUID;
    private final UUID messageUUID;
    private final QueueMessage deleted;

    private TransactionDeleteResponse(UUID transactionUUID, UUID messageUUID, QueueMessage deleted) {
        this.transactionUUID = transactionUUID;
        this.messageUUID = messageUUID;
        this.deleted = deleted;
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

    public static class Builder implements EntryBuilder<Builder, TransactionDeleteResponse> {
        private UUID transactionUUID;
        private UUID messageUUID;
        private QueueMessage deleted;

        @Override
        public Builder from(TransactionDeleteResponse entry) {
            transactionUUID = entry.transactionUUID;
            messageUUID = entry.messageUUID;
            deleted = entry.deleted;
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

        public Builder setDeleted(QueueMessage deleted) {
            this.deleted = deleted;
            return this;
        }

        @Override
        public TransactionDeleteResponse build() {
            return new TransactionDeleteResponse(transactionUUID, messageUUID, deleted);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeUUID(transactionUUID);
            stream.writeUUID(messageUUID);
            stream.writeStreamable(deleted);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            transactionUUID = stream.readUUID();
            messageUUID = stream.readUUID();
            deleted = stream.readStreamable(QueueMessage::new);
        }
    }
}
