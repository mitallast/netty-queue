package org.mitallast.queue.crdt.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

public class AppendRejected implements Streamable {
    private final int bucket;
    private final long replica;
    private final long vclock;

    public AppendRejected(int bucket, long replica, long vclock) {
        this.bucket = bucket;
        this.replica = replica;
        this.vclock = vclock;
    }

    public AppendRejected(StreamInput stream) {
        bucket = stream.readInt();
        replica = stream.readLong();
        vclock = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(bucket);
        stream.writeLong(replica);
        stream.writeLong(vclock);
    }

    public int bucket() {
        return bucket;
    }

    public long replica() {
        return replica;
    }

    public long vclock() {
        return vclock;
    }
}
