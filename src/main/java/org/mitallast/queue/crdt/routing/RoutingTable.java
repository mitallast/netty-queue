package org.mitallast.queue.crdt.routing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RoutingTable implements Streamable {
    private final int replicas;
    private final ImmutableSet<DiscoveryNode> members;
    private final ImmutableList<RoutingBucket> buckets;

    public RoutingTable(int replicas, int buckets) {
        this(replicas, ImmutableSet.of(), Immutable.generate(buckets, RoutingBucket::new));
    }

    public RoutingTable(int replicas, ImmutableSet<DiscoveryNode> members, ImmutableList<RoutingBucket> buckets) {
        this.replicas = replicas;
        this.members = members;
        this.buckets = buckets;
    }

    public RoutingTable(StreamInput stream) throws IOException {
        replicas = stream.readInt();
        members = stream.readStreamableSet(DiscoveryNode::new);
        buckets = stream.readStreamableList(RoutingBucket::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeInt(replicas);
        stream.writeStreamableList(members);
        stream.writeStreamableList(buckets);
    }

    public int replicas() {
        return replicas;
    }

    public ImmutableSet<DiscoveryNode> members() {
        return members;
    }

    public ImmutableList<RoutingBucket> buckets() {
        return buckets;
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
            Immutable.replace(buckets, bucket.index(), bucket)
        );
    }

    public RoutingTable withoutResource(long id) {
        RoutingBucket bucket = bucket(id).withoutResource(id);
        return new RoutingTable(
            replicas,
            members,
            Immutable.replace(buckets, bucket.index(), bucket)
        );
    }

    public RoutingTable withBucketMember(int bucket, DiscoveryNode node) {
        RoutingBucket updated = buckets.get(bucket).withMember(node);
        return new RoutingTable(
            replicas,
            members,
            Immutable.replace(buckets, bucket, updated)
        );
    }

    public RoutingTable withMembers(ImmutableSet<DiscoveryNode> members) {
        return new RoutingTable(
            replicas,
            members,
            Immutable.map(buckets, bucket -> bucket.filterMembers(members))
        );
    }
}
