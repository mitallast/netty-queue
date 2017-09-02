package org.mitallast.queue.crdt.commutative;

import javaslang.collection.LinkedHashSet;
import javaslang.collection.Set;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.replication.Replicator;

public class GSet implements CmRDT {

    public static class SourceAdd implements SourceUpdate {
        public static final Codec<SourceAdd> codec = Codec.Companion.of(
            SourceAdd::new,
            SourceAdd::value,
            Codec.Companion.anyCodec()
        );

        private final Message value;

        public SourceAdd(Message value) {
            this.value = value;
        }

        public Message value() {
            return value;
        }
    }

    public static class DownstreamAdd implements DownstreamUpdate {
        public static final Codec<DownstreamAdd> codec = Codec.Companion.of(
            DownstreamAdd::new,
            DownstreamAdd::value,
            Codec.Companion.anyCodec()
        );

        private final Message value;

        public DownstreamAdd(Message value) {
            this.value = value;
        }

        public Message value() {
            return value;
        }
    }

    private final long id;
    private final Replicator replicator;

    private volatile Set<Message> values = LinkedHashSet.empty();

    public GSet(long id, Replicator replicator) {
        this.id = id;
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

    public synchronized void add(Message value) {
        if (!values.contains(value)) {
            values = values.add(value);
            replicator.append(id, new DownstreamAdd(value));
        }
    }

    public Set<Message> values() {
        return values;
    }
}
