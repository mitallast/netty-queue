package org.mitallast.queue.crdt.commutative;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.Optional;
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

    private final Consumer<Streamable> broadcast;

    private Streamable value = null;
    private long timestamp = 0;

    public LWWRegister(Consumer<Streamable> broadcast) {
        this.broadcast = broadcast;
    }

    @Override
    public void update(Streamable event) {
        if (event instanceof SourceUpdate) {
            sourceUpdate((SourceUpdate) event);
        } else if (event instanceof DownstreamUpdate) {
            downstreamUpdate((DownstreamUpdate) event);
        }
    }

    @Override
    public boolean shouldCompact(Streamable event) {
        return event instanceof DownstreamAssign &&
            ((DownstreamAssign) event).timestamp < timestamp;
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
            synchronized (this) {
                if (set.timestamp > timestamp) {
                    value = set.value;
                    timestamp = set.timestamp;
                }
            }
        }
    }

    public void assign(Streamable value) {
        synchronized (this) {
            this.value = value;
            this.timestamp = System.currentTimeMillis();
        }
        broadcast.accept(new DownstreamAssign(value, timestamp));
    }

    public Optional<Streamable> value() {
        return Optional.ofNullable(value);
    }
}
