package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class CloseBucketMember implements Streamable {
    private final int bucket;
    private final DiscoveryNode member;

    public CloseBucketMember(int bucket, DiscoveryNode member) {
        this.bucket = bucket;
        this.member = member;
    }

    public CloseBucketMember(StreamInput stream) {
        bucket = stream.readInt();
        member = stream.readStreamable(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(bucket);
        stream.writeStreamable(member);
    }

    public int bucket() {
        return bucket;
    }

    public DiscoveryNode member() {
        return member;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CloseBucketMember that = (CloseBucketMember) o;

        if (bucket != that.bucket) return false;
        return member.equals(that.member);
    }

    @Override
    public int hashCode() {
        int result = bucket;
        result = 31 * result + member.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CloseBucketMember{bucket=" + bucket + ", " + member + '}';
    }
}
