package org.mitallast.queue.crdt.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

public class AppendSuccessful implements Streamable {
    private final int bucket;
    private final long replica;
    private final long index;

    public AppendSuccessful(int bucket, long replica, long index) {
        this.bucket = bucket;
        this.replica = replica;
        this.index = index;
    }

    public AppendSuccessful(StreamInput stream) {
        bucket = stream.readInt();
        replica = stream.readLong();
        index = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(bucket);
        stream.writeLong(replica);
        stream.writeLong(index);
    }

    public int bucket() {
        return bucket;
    }

    public long replica() {
        return replica;
    }

    public long index() {
        return index;
    }

    @Override
    public String toString() {
        return "AppendSuccessful{" +
            "bucket=" + bucket +
            ", replica=" + replica +
            ", index=" + index +
            '}';
    }
}
