package org.mitallast.queue.crdt.routing;

import javaslang.collection.HashSet;
import javaslang.collection.Set;
import javaslang.collection.Vector;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;


public class RoutingTable implements Streamable {
    private final int replicas;
    private final Set<DiscoveryNode> members;
    private final Vector<RoutingBucket> buckets;

    public RoutingTable(int replicas, int buckets) {
        this(replicas, HashSet.empty(), Vector.range(0, buckets).map(RoutingBucket::new));
    }

    public RoutingTable(int replicas, Set<DiscoveryNode> members, Vector<RoutingBucket> buckets) {
        this.replicas = replicas;
        this.members = members;
        this.buckets = buckets;
    }

    public RoutingTable(StreamInput stream) {
        replicas = stream.readInt();
        members = stream.readSet(DiscoveryNode::new);
        buckets = stream.readVector(RoutingBucket::new);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(replicas);
        stream.writeSet(members);
        stream.writeVector(buckets);
    }

    public int replicas() {
        return replicas;
    }

    public Set<DiscoveryNode> members() {
        return members;
    }

    public Vector<RoutingBucket> buckets() {
        return buckets;
    }

    public int bucketsCount(DiscoveryNode node) {
        return buckets.count(bucket -> bucket.members().containsKey(node));
    }

    public RoutingBucket bucket(long resourceId) {
        int bucket = Long.hashCode(resourceId) % buckets.size();
        return buckets.get(bucket);
    }

    public Resource resource(long id) {
        return bucket(id).resource(id);
    }

    public boolean hasResource(long id) {
        return bucket(id).hasResource(id);
    }

    public RoutingTable withResource(Resource resource) {
        RoutingBucket bucket = bucket(resource.id()).withResource(resource);
        return new RoutingTable(
            replicas,
            members,
            buckets.update(bucket.index(), bucket)
        );
    }

    public RoutingTable withoutResource(long id) {
        RoutingBucket bucket = bucket(id).withoutResource(id);
        return new RoutingTable(
            replicas,
            members,
            buckets.update(bucket.index(), bucket)
        );
    }

    public RoutingTable withBucketMember(int bucket, DiscoveryNode node) {
        RoutingBucket updated = buckets.get(bucket).withMember(node);
        return new RoutingTable(
            replicas,
            members,
            buckets.update(bucket, updated)
        );
    }

    public RoutingTable withBucketMember(int bucket, BucketMember node) {
        RoutingBucket updated = buckets.get(bucket).withMember(node);
        return new RoutingTable(
            replicas,
            members,
            buckets.update(bucket, updated)
        );
    }

    public RoutingTable withMembers(Set<DiscoveryNode> members) {
        return new RoutingTable(
            replicas,
            members,
            buckets.map(bucket -> bucket.filterMembers(members))
        );
    }

    public RoutingTable withoutBucketMember(int bucket, DiscoveryNode member) {
        RoutingBucket updated = buckets.get(bucket).withoutMember(member);
        return new RoutingTable(
            replicas,
            members,
            buckets.update(bucket, updated)
        );
    }

    @Override
    public String toString() {
        return "RoutingTable{" +
            "replicas=" + replicas +
            ", members=" + members +
            ", buckets=" + buckets +
            '}';
    }
}
