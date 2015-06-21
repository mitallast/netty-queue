package org.mitallast.queue.action.queue.transactional.delete;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;
import java.util.UUID;

public class TransactionDeleteRequest implements ActionRequest<TransactionDeleteRequest.Builder, TransactionDeleteRequest> {
    private final UUID transactionUUID;
    private final String queue;
    private final UUID messageUUID;

    private TransactionDeleteRequest(UUID transactionUUID, String queue, UUID messageUUID) {
        this.transactionUUID = transactionUUID;
        this.queue = queue;
        this.messageUUID = messageUUID;
    }

    public UUID transactionUUID() {
        return transactionUUID;
    }

    public String queue() {
        return queue;
    }

    public UUID messageUUID() {
        return messageUUID;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("transactionUUID", transactionUUID)
            .missing("queue", queue)
            .missing("messageUUID", messageUUID);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, TransactionDeleteRequest> {
        private UUID transactionUUID;
        private String queue;
        private UUID messageUUID;

        @Override
        public Builder from(TransactionDeleteRequest entry) {
            transactionUUID = entry.transactionUUID;
            queue = entry.queue;
            messageUUID = entry.messageUUID;
            return this;
        }

        public Builder setTransactionUUID(UUID transactionUUID) {
            this.transactionUUID = transactionUUID;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder setMessageUUID(UUID messageUUID) {
            this.messageUUID = messageUUID;
            return this;
        }

        @Override
        public TransactionDeleteRequest build() {
            return new TransactionDeleteRequest(transactionUUID, queue, messageUUID);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            transactionUUID = stream.readUUID();
            queue = stream.readText();
            messageUUID = stream.readUUID();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeUUID(transactionUUID);
            stream.writeText(queue);
            stream.writeUUID(messageUUID);
        }
    }
}
