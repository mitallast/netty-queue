package org.mitallast.queue.action.queue.transactional.commit;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

public class TransactionCommitResponse implements ActionResponse<TransactionCommitResponse.Builder, TransactionCommitResponse> {
    private final UUID transactionUUID;
    private final String queue;

    private TransactionCommitResponse(UUID transactionUUID, String queue) {
        this.transactionUUID = transactionUUID;
        this.queue = queue;
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

    public static class Builder implements EntryBuilder<Builder, TransactionCommitResponse> {
        private UUID transactionUUID;
        private String queue;

        @Override
        public Builder from(TransactionCommitResponse entry) {
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
        public TransactionCommitResponse build() {
            return new TransactionCommitResponse(transactionUUID, queue);
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
