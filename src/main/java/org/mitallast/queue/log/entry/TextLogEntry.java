package org.mitallast.queue.log.entry;

import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class TextLogEntry extends LogEntry<TextLogEntry> {
    private final String message;

    public TextLogEntry(long index, String message) {
        super(index);
        this.message = message;
    }

    public String message() {
        return message;
    }

    @Override
    public EntryBuilder<TextLogEntry> toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends LogEntry.Builder<Builder, TextLogEntry> {
        private String message;

        @Override
        public Builder from(TextLogEntry entry) {
            message = entry.message;
            return super.from(entry);
        }

        public Builder setMessage(String message) {
            this.message = message;
            return this;
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            super.readFrom(stream);
            message = stream.readText();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            super.writeTo(stream);
            stream.writeText(message);
        }

        @Override
        public TextLogEntry build() {
            return new TextLogEntry(index, message);
        }
    }
}
