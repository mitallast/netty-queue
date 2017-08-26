package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class CloseReplica implements Message {
    public static final Codec<CloseReplica> codec = Codec.of(
        CloseReplica::new,
        CloseReplica::bucket,
        CloseReplica::replica,
        Codec.intCodec,
        Codec.longCodec
    );

    private final int bucket;
    private final long replica;

    public CloseReplica(int bucket, long replica) {
        this.bucket = bucket;
        this.replica = replica;
    }

    public int bucket() {
        return bucket;
    }

    public long replica() {
        return replica;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CloseReplica that = (CloseReplica) o;

        if (bucket != that.bucket) return false;
        return replica == that.replica;
    }

    @Override
    public int hashCode() {
        int result = bucket;
        result = 31 * result + (int) (replica ^ (replica >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CloseReplica{bucket=" + bucket + ", replica=" + replica + '}';
    }
}
