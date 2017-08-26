package org.mitallast.queue.crdt.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class AppendSuccessful implements Message {
    public static final Codec<AppendSuccessful> codec = Codec.of(
        AppendSuccessful::new,
        AppendSuccessful::bucket,
        AppendSuccessful::replica,
        AppendSuccessful::index,
        Codec.intCodec,
        Codec.longCodec,
        Codec.longCodec
    );

    private final int bucket;
    private final long replica;
    private final long index;

    public AppendSuccessful(int bucket, long replica, long index) {
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

    @Override
    public String toString() {
        return "AppendSuccessful{" +
            "bucket=" + bucket +
            ", replica=" + replica +
            ", index=" + index +
            '}';
    }
}
