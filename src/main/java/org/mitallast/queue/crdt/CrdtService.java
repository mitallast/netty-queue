package org.mitallast.queue.crdt;

import com.google.common.collect.ImmutableCollection;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.routing.RoutingTable;

public interface CrdtService {

    RoutingTable routingTable();

    boolean contains(int index);

    Bucket bucket(int index);

    Bucket bucket(long resourceId);

    ImmutableCollection<Bucket> buckets();
}
