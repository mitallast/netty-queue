package org.mitallast.queue.crdt.rest;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import javaslang.collection.Vector;
import javaslang.concurrent.Future;
import javaslang.control.Option;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.json.JsonMessage;
import org.mitallast.queue.crdt.CrdtService;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.commutative.OrderedGSet;
import org.mitallast.queue.crdt.routing.ResourceType;
import org.mitallast.queue.rest.RestController;

public class RestOrderedGSet {
    private final CrdtService crdtService;

    @Inject
    public RestOrderedGSet(
        RestController controller,
        CrdtService crdtService
    ) {
        this.crdtService = crdtService;

        controller.handle(this::create)
            .apply(controller.param().toLong("id"))
            .apply(controller.response().futureEither(
                controller.response().created(),
                controller.response().badRequest()
            ))
            .handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/ordered-g-set");

        controller.handle(this::values)
            .apply(controller.param().toLong("id"))
            .apply(controller.response().optionalJson())
            .handle(HttpMethod.GET, "_crdt/{id}/ordered-g-set/values");

        controller.handle(this::add)
            .apply(controller.param().toLong("id"))
            .apply(controller.param().toLong("timestamp"))
            .apply(controller.param().json(JsonMessage.class))
            .apply(controller.response().either(
                controller.response().ok(),
                controller.response().badRequest()
            ))
            .handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/ordered-g-set/add");
    }

    public Future<Boolean> create(long id) {
        return crdtService.addResource(id, ResourceType.OrderedGSet);
    }

    public Option<Vector<Message>> values(long id) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return Option.none();
        } else {
            return bucket.registry().crdtOpt(id, OrderedGSet.class).map(OrderedGSet::values);
        }
    }

    public boolean add(long id, long timestamp, JsonMessage value) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return false;
        }
        Option<OrderedGSet> set = bucket.registry().crdtOpt(id, OrderedGSet.class);
        if (set.isDefined()) {
            set.get().add(value, timestamp);
        }
        return true;
    }
}
