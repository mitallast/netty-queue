package org.mitallast.queue.crdt.commutative;

import com.google.common.base.Preconditions;
import gnu.trove.impl.sync.TSynchronizedLongLongMap;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.procedure.TLongProcedure;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.replication.Replicator;

/**
 * Using counter vector allows to implement garbage collection
 */
public class GCounter implements CmRDT {

    public static class SourceAssign implements SourceUpdate {
        private final long value;

        public SourceAssign(long value) {
            this.value = value;
        }

        public SourceAssign(StreamInput stream) {
            this.value = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeLong(value);
        }
    }

    public static class DownstreamAssign implements DownstreamUpdate {
        private final long replica;
        private final long value;

        public DownstreamAssign(long replica, long value) {
            this.replica = replica;
            this.value = value;
        }

        public DownstreamAssign(StreamInput stream) {
            this.replica = stream.readLong();
            this.value = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeLong(replica);
            stream.writeLong(value);
        }
    }

    private final long id;
    private final long replica;
    private final Replicator replicator;
    private final TLongLongMap counterMap;

    public GCounter(long id, long replica, Replicator replicator) {
        this.id = id;
        this.replica = replica;
        this.replicator = replicator;
        this.counterMap = new TSynchronizedLongLongMap(new TLongLongHashMap());
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
            ((DownstreamAssign) event).value < counterMap.get(replica);
    }

    @Override
    public void sourceUpdate(SourceUpdate update) {
        if (update instanceof SourceAssign) {
            add(((SourceAssign) update).value);
        }
    }

    @Override
    public synchronized void downstreamUpdate(DownstreamUpdate update) {
        if (update instanceof DownstreamAssign) {
            DownstreamAssign downstream = (DownstreamAssign) update;
            long current = counterMap.get(downstream.replica);
            if (current < downstream.value) {
                counterMap.put(downstream.replica, downstream.value);
            }
        }
    }

    public long increment() {
        return add(1);
    }

    public long add(long value) {
        Preconditions.checkArgument(value >= 0, "must be positive");
        long updated = counterMap.adjustOrPutValue(replica, value, value);
        replicator.append(id, new DownstreamAssign(replica, updated));
        return updated;
    }

    public long value() {
        SumProcedure sum = new SumProcedure();
        counterMap.forEachValue(sum);
        return sum.value;
    }

    private static class SumProcedure implements TLongProcedure {
        long value = 0;

        @Override
        public boolean execute(long value) {
            this.value += value;
            return true;
        }
    }
}
