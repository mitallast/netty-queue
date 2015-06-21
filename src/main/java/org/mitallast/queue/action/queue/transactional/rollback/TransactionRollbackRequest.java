package org.mitallast.queue.action.queue.transactional.rollback;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;
import java.util.UUID;

public class TransactionRollbackRequest implements ActionRequest<TransactionRollbackRequest.Builder, TransactionRollbackRequest> {
    private final UUID transactionUUID;
    private final String queue;

    private TransactionRollbackRequest(UUID transactionUUID, String queue) {
        this.transactionUUID = transactionUUID;
        this.queue = queue;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("transactionUUID", transactionUUID)
            .missing("queue", queue);
    }

    public UUID transactionUUID() {
        return transactionUUID;
    }

    public String queue() {
        return queue;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, TransactionRollbackRequest> {
        private UUID transactionUUID;
        private String queue;

        @Override
        public Builder from(TransactionRollbackRequest entry) {
            transactionUUID = entry.transactionUUID;
            queue = entry.queue;
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

        @Override
        public TransactionRollbackRequest build() {
            return new TransactionRollbackRequest(transactionUUID, queue);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            transactionUUID = stream.readUUID();
            queue = stream.readText();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeText(queue);
            stream.writeUUID(transactionUUID);
        }
    }
}
