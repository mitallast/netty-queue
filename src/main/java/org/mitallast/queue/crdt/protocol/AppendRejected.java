package org.mitallast.queue.crdt.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class AppendRejected implements Message {
    public static final Codec<AppendRejected> codec = Codec.of(
        AppendRejected::new,
        AppendRejected::bucket,
        AppendRejected::replica,
        AppendRejected::index,
        Codec.intCodec,
        Codec.longCodec,
        Codec.longCodec
    );

    private final int bucket;
    private final long replica;
    private final long index;

    public AppendRejected(int bucket, long replica, long index) {
        this.bucket = bucket;
        this.replica = replica;
        this.index = index;
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
}
