package org.mitallast.queue.crdt.commutative;

import javaslang.control.Option;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.replication.Replicator;

public class LWWRegister implements CmRDT {

    public static class SourceAssign implements SourceUpdate {
        public static final Codec<SourceAssign> codec = Codec.of(
            SourceAssign::new,
            SourceAssign::value,
            SourceAssign::timestamp,
            Codec.anyCodec(),
            Codec.longCodec
        );

        private final Message value;
        private final long timestamp;

        public SourceAssign(Message value, long timestamp) {
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

    public static class DownstreamAssign implements DownstreamUpdate {
        public static final Codec<DownstreamAssign> codec = Codec.of(
            DownstreamAssign::new,
            DownstreamAssign::value,
            DownstreamAssign::timestamp,
            Codec.anyCodec(),
            Codec.longCodec
        );

        private final Message value;
        private final long timestamp;

        public DownstreamAssign(Message value, long timestamp) {
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

    private final long id;
    private final Replicator replicator;

    private volatile Option<Message> value = Option.none();
    private volatile long timestamp = 0;

    public LWWRegister(long id, Replicator replicator) {
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

    public synchronized void assign(Message value, long timestamp) {
        if (this.timestamp < timestamp) {
            this.value = Option.some(value);
            this.timestamp = timestamp;
            replicator.append(id, new DownstreamAssign(value, timestamp));
        }
    }

    public Option<Message> value() {
        return value;
    }
}
