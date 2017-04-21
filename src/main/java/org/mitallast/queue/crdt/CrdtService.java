package org.mitallast.queue.crdt;

import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.routing.RoutingTable;

public interface CrdtService {

    RoutingTable routingTable();

    Bucket bucket(int index);

    Bucket bucket(long resourceId);
}
