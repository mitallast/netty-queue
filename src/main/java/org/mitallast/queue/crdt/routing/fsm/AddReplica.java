package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class AddReplica implements Message {
    public static final Codec<AddReplica> codec = Codec.of(
        AddReplica::new,
        AddReplica::bucket,
        AddReplica::member,
        Codec.intCodec,
        DiscoveryNode.codec
    );

    private final int bucket;
    private final DiscoveryNode member;

    public AddReplica(int bucket, DiscoveryNode member) {
        this.bucket = bucket;
        this.member = member;
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

        AddReplica that = (AddReplica) o;

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
        return "AddReplica{bucket=" + bucket + ", " + member + '}';
    }
}
