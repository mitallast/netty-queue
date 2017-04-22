package org.mitallast.queue.crdt.commutative;

import javaslang.control.Option;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.replication.Replicator;

public class LWWRegister implements CmRDT {

    public static class SourceAssign implements SourceUpdate {

        private final Streamable value;
        private final long timestamp;

        public SourceAssign(Streamable value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public SourceAssign(StreamInput stream) {
            this.value = stream.readStreamable();
            this.timestamp = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeClass(value.getClass());
            stream.writeStreamable(value);
            stream.writeLong(timestamp);
        }
    }

    public static class DownstreamAssign implements DownstreamUpdate {

        private final Streamable value;
        private final long timestamp;

        public DownstreamAssign(Streamable value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public DownstreamAssign(StreamInput stream) {
            this.value = stream.readStreamable();
            this.timestamp = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeClass(value.getClass());
            stream.writeStreamable(value);
            stream.writeLong(timestamp);
        }

    }

    private final long id;
    private final Replicator replicator;

    private volatile Option<Streamable> value = Option.none();
    private volatile long timestamp = 0;

    public LWWRegister(long id, Replicator replicator) {
        this.id = id;
        this.replicator = replicator;
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
            SourceAssign assign = (SourceAssign) update;
            assign(assign.value, assign.timestamp);
        }
    }

    @Override
    public void downstreamUpdate(DownstreamUpdate update) {
        if (update instanceof DownstreamAssign) {
            DownstreamAssign set = (DownstreamAssign) update;
            synchronized (this) {
                if (set.timestamp > timestamp) {
                    value = Option.some(set.value);
                    timestamp = set.timestamp;
                }
            }
        }
    }

    public synchronized void assign(Streamable value, long timestamp) {
        if (this.timestamp < timestamp) {
            this.value = Option.some(value);
            this.timestamp = timestamp;
            replicator.append(id, new DownstreamAssign(value, timestamp));
        }
    }

    public Option<Streamable> value() {
        return value;
    }
}
