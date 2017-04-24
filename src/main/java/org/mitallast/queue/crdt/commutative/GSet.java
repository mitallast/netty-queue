package org.mitallast.queue.crdt.commutative;

import javaslang.collection.LinkedHashSet;
import javaslang.collection.Set;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.replication.Replicator;

public class GSet implements CmRDT {

    public static class SourceAdd implements SourceUpdate {
        private final Streamable value;

        public SourceAdd(Streamable value) {
            this.value = value;
        }

        public SourceAdd(StreamInput stream) {
            this.value = stream.readStreamable();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeClass(value.getClass());
            stream.writeStreamable(value);
        }
    }

    public static class DownstreamAdd implements DownstreamUpdate {
        private final Streamable value;

        public DownstreamAdd(Streamable value) {
            this.value = value;
        }

        public DownstreamAdd(StreamInput stream) {
            this.value = stream.readStreamable();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeClass(value.getClass());
            stream.writeStreamable(value);
        }

        @Override
        public String toString() {
            return "DownstreamAdd{" +
                "value=" + value +
                '}';
        }
    }

    private final long id;
    private final Replicator replicator;

    private volatile Set<Streamable> values = LinkedHashSet.empty();

    public GSet(long id, Replicator replicator) {
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
        return false;
    }

    @Override
    public void sourceUpdate(SourceUpdate update) {
        if (update instanceof SourceAdd) {
            SourceAdd add = (SourceAdd) update;
            add(add.value);
        }
    }

    @Override
    public synchronized void downstreamUpdate(DownstreamUpdate update) {
        if (update instanceof DownstreamAdd) {
            DownstreamAdd add = (DownstreamAdd) update;
            if (!values.contains(add.value)) {
                values = values.add(add.value);
            }
        }
    }

    public synchronized void add(Streamable value) {
        if (!values.contains(value)) {
            values = values.add(value);
            replicator.append(id, new DownstreamAdd(value));
        }
    }

    public Set<Streamable> values() {
        return values;
    }
}
