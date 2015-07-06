package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.StateMachine;
import org.mitallast.queue.raft.resource.result.LongResult;

import java.io.IOException;

public class CreateResource extends PathOperation<LongResult, CreateResource> implements Command<LongResult> {

    private Class<? extends StateMachine> type;

    public CreateResource(String path, Class<? extends StateMachine> type) {
        super(path);
        this.type = type;
    }

    public Class<? extends StateMachine> type() {
        return type;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PathOperation.Builder<LongResult, Builder, CreateResource> {
        private Class<? extends StateMachine> type;

        @Override
        public Builder from(CreateResource entry) {
            super.from(entry);
            type = entry.type;
            return this;
        }

        public Builder setType(Class<? extends StateMachine> type) {
            this.type = type;
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void readFrom(StreamInput stream) throws IOException {
            super.readFrom(stream);
            String className = stream.readText();
            try {
                type = (Class<? extends StateMachine>) getClass().getClassLoader().loadClass(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void writeTo(StreamOutput stream) throws IOException {
            super.writeTo(stream);
            stream.writeText(type.getName());
        }

        @Override
        public CreateResource build() {
            return new CreateResource(path, type);
        }
    }
}
