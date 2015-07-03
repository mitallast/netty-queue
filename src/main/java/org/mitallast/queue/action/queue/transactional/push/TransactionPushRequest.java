package org.mitallast.queue.action.queue.transactional.push;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;
import java.util.UUID;

public class TransactionPushRequest implements ActionRequest<TransactionPushRequest> {
    private final UUID transactionUUID;
    private final String queue;
    private final QueueMessage message;

    private TransactionPushRequest(UUID transactionUUID, String queue, QueueMessage message) {
        this.transactionUUID = transactionUUID;
        this.queue = queue;
        this.message = message;
    }

    public UUID transactionUUID() {
        return transactionUUID;
    }

    public String queue() {
        return queue;
    }

    public QueueMessage message() {
        return message;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder().missing("transactionUUID", transactionUUID);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<TransactionPushRequest> {
        private UUID transactionUUID;
        private String queue;
        private QueueMessage message;

        private Builder from(TransactionPushRequest entry) {
            transactionUUID = entry.transactionUUID;
            queue = entry.queue;
            message = entry.message;
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

        public Builder setMessage(QueueMessage message) {
            this.message = message;
            return this;
        }

        @Override
        public TransactionPushRequest build() {
            return new TransactionPushRequest(transactionUUID, queue, message);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeUUID(transactionUUID);
            stream.writeText(queue);
            stream.writeStreamable(message);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            transactionUUID = stream.readUUID();
            queue = stream.readText();
            message = stream.readStreamable(QueueMessage::new);
        }
    }
}
