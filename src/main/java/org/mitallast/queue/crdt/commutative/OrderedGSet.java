package org.mitallast.queue.crdt.commutative;

import javaslang.collection.Set;
import javaslang.collection.TreeSet;
import javaslang.collection.Vector;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.replication.Replicator;

/**
 * Like a G-Set, but require unique timestamp per replica.
 * If two replicas contains equal timestamp, entries will be sorted by replica id for stable sorting.
 */
public class OrderedGSet implements CmRDT {

    public static class SourceAdd implements SourceUpdate {
        public static final Codec<SourceAdd> codec = Codec.of(
            SourceAdd::new,
            SourceAdd::value,
            SourceAdd::timestamp,
            Codec.anyCodec(),
            Codec.longCodec
        );

        private final Message value;
        private final long timestamp;

        public SourceAdd(Message value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public Message value() {
            return value;
        }

        public long timestamp() {
            return timestamp;
        }
    }

    public static class DownstreamAdd implements DownstreamUpdate {
        public static final Codec<DownstreamAdd> codec = Codec.of(
            DownstreamAdd::new,
            DownstreamAdd::value,
            DownstreamAdd::timestamp,
            DownstreamAdd::replica,
            Codec.anyCodec(),
            Codec.longCodec,
            Codec.longCodec
        );

        private final Message value;
        private final long timestamp;
        private final long replica;

        public DownstreamAdd(Message value, long timestamp, long replica) {
            this.value = value;
            this.timestamp = timestamp;
            this.replica = replica;
        }

        public Message value() {
            return value;
        }

        public long timestamp() {
            return timestamp;
        }

        public long replica() {
            return replica;
        }
    }

    private static class Entry {
        private final Message value;
        private final long timestamp;
        private final long replica;

        public Entry(Message value, long timestamp, long replica) {
            this.value = value;
            this.timestamp = timestamp;
            this.replica = replica;
        }
    }

    private final long id;
    private final long replica;
    private final Replicator replicator;

    private volatile Set<Entry> values = TreeSet.empty((o1, o2) -> {
        int c = Long.compare(o1.timestamp, o2.timestamp);
        if (c == 0) {
            c = Long.compare(o1.replica, o2.replica);
        }
        return c;
    });

    public OrderedGSet(long id, long replica, Replicator replicator) {
        this.id = id;
        this.replica = replica;
        this.replicator = replicator;
    }

    @Override
    public void update(Message event) {
        if (event instanceof SourceUpdate) {
            sourceUpdate((SourceUpdate) event);
        } else if (event instanceof DownstreamUpdate) {
            downstreamUpdate((DownstreamUpdate) event);
        }
    }

    @Override
    public boolean shouldCompact(Message event) {
        return false;
    }

    @Override
    public void sourceUpdate(SourceUpdate update) {
        if (update instanceof SourceAdd) {
            SourceAdd add = (SourceAdd) update;
            add(add.value, add.timestamp);
        }
    }

    @Override
    public synchronized void downstreamUpdate(DownstreamUpdate update) {
        if (update instanceof DownstreamAdd) {
            DownstreamAdd add = (DownstreamAdd) update;
            Entry entry = new Entry(add.value, add.timestamp, add.replica);
            if (!values.contains(entry)) {
                values = values.add(entry);
            }
        }
    }

    public synchronized void add(Message value, long timestamp) {
        Entry entry = new Entry(value, timestamp, replica);
        if (!values.contains(entry)) {
            values = values.add(entry);
            replicator.append(id, new DownstreamAdd(value, timestamp, replica));
        }
    }

    public Vector<Message> values() {
        return values.toVector().map(e -> e.value);
    }
}

