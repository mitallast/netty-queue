package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

public class RemoveReplica implements Streamable {
    private final int bucket;
    private final long replica;

    public RemoveReplica(int bucket, long replica) {
        this.bucket = bucket;
        this.replica = replica;
    }

    public RemoveReplica(StreamInput stream) {
        bucket = stream.readInt();
        replica = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(bucket);
        stream.writeLong(replica);
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

        RemoveReplica that = (RemoveReplica) o;

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
        return "RemoveReplica{bucket=" + bucket + ", replica=" + replica + '}';
    }
}
