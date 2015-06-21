package org.mitallast.queue.action.queue.transactional.pop;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;
import java.util.UUID;

public class TransactionPopRequest implements ActionRequest<TransactionPopRequest.Builder, TransactionPopRequest> {
    private final UUID transactionUUID;
    private final String queue;

    private TransactionPopRequest(UUID transactionUUID, String queue) {
        this.transactionUUID = transactionUUID;
        this.queue = queue;
    }

    public String queue() {
        return queue;
    }

    public UUID transactionUUID() {
        return transactionUUID;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("transactionUUID", transactionUUID)
            .missing("queue", queue);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, TransactionPopRequest> {
        private UUID transactionUUID;
        private String queue;

        @Override
        public Builder from(TransactionPopRequest entry) {
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
        public TransactionPopRequest build() {
            return new TransactionPopRequest(transactionUUID, queue);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            transactionUUID = stream.readUUID();
            queue = stream.readText();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeUUID(transactionUUID);
            stream.writeText(queue);
        }
    }
}
