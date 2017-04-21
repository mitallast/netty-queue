package org.mitallast.queue.crdt;

import javaslang.concurrent.Future;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.routing.ResourceType;
import org.mitallast.queue.crdt.routing.RoutingTable;

public interface CrdtService {

    Future<Boolean> addResource(long id, ResourceType resourceType);

    RoutingTable routingTable();

    Bucket bucket(int index);

    Bucket bucket(long resourceId);
}
