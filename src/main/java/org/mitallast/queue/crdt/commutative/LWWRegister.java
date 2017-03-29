package org.mitallast.queue.crdt.commutative;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.function.Consumer;

public class LWWRegister implements CmRDT<LWWRegister> {

    public static class SourceAssign implements SourceUpdate {
        private final Streamable value;

        public SourceAssign(Streamable value) {
            this.value = value;
        }

        public SourceAssign(StreamInput stream) throws IOException {
            this.value = stream.readStreamable();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeClass(value.getClass());
            stream.writeStreamable(value);
        }
    }

    public static class DownstreamAssign implements DownstreamUpdate {
        private final Streamable value;
        private final long timestamp;

        public DownstreamAssign(Streamable value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public DownstreamAssign(StreamInput stream) throws IOException {
            this.value = stream.readStreamable();
            this.timestamp = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeClass(value.getClass());
            stream.writeStreamable(value);
            stream.writeLong(timestamp);
        }
    }

    public static class Value implements Query {

        public static final Value INSTANCE = new Value();

        public static Value read(StreamInput stream) throws IOException {
            return INSTANCE;
        }

        private Value() {
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {}
    }

    public static class ValueResponse implements QueryResponse {
        private final Streamable value;

        public ValueResponse(Streamable value) {
            this.value = value;
        }

        public ValueResponse(StreamInput stream) throws IOException {
            if (stream.readBoolean()) {
                this.value = stream.readStreamable();
            } else {
                this.value = null;
            }
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            if (value == null) {
                stream.writeBoolean(false);
            } else {
                stream.writeBoolean(true);
                stream.writeClass(value.getClass());
                stream.writeStreamableOrNull(value);
            }
        }

        public Streamable value() {
            return value;
        }
    }

    private final Consumer<Streamable> broadcast;
    private volatile Streamable value = null;
    private volatile long timestamp = 0;

    public LWWRegister(Consumer<Streamable> broadcast) {
        this.broadcast = broadcast;
    }

    @Override
    public void sourceUpdate(SourceUpdate update) {
        if (update instanceof SourceAssign) {
            assign(((SourceAssign) update).value);
        }
    }

    @Override
    public void downstreamUpdate(DownstreamUpdate update) {
        if (update instanceof DownstreamAssign) {
            DownstreamAssign set = (DownstreamAssign) update;
            if (set.timestamp > timestamp) {
                value = set.value;
                timestamp = set.timestamp;
            }
        }
    }

    public void assign(Streamable value) {
        this.value = value;
        this.timestamp = System.currentTimeMillis();
        broadcast.accept(new DownstreamAssign(value, timestamp));
    }

    @Override
    public QueryResponse query(Query query) {
        if (query instanceof Value) {
            return new ValueResponse(value());
        }
        return null;
    }

    public Streamable value() {
        return value;
    }
}
