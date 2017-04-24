package org.mitallast.queue.crdt.rest;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import javaslang.collection.Set;
import javaslang.concurrent.Future;
import javaslang.control.Option;
import org.mitallast.queue.common.json.JsonStreamable;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.CrdtService;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.commutative.GSet;
import org.mitallast.queue.crdt.routing.ResourceType;
import org.mitallast.queue.rest.RestController;

public class RestGSet {
    private final CrdtService crdtService;

    @Inject
    public RestGSet(
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
            .handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-counter");

        controller.handle(this::values)
            .apply(controller.param().toLong("id"))
            .apply(controller.response().optionalJson())
            .handle(HttpMethod.GET, "_crdt/{id}/g-counter/values");

        controller.handle(this::add)
            .apply(controller.param().toLong("id"))
            .apply(controller.param().json(JsonStreamable.class))
            .apply(controller.response().either(
                controller.response().ok(),
                controller.response().badRequest()
            ))
            .handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/lww-register/add");
    }

    public Future<Boolean> create(long id) {
        return crdtService.addResource(id, ResourceType.GSet);
    }

    public Option<Set<Streamable>> values(long id) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return Option.none();
        } else {
            return bucket.registry().crdtOpt(id, GSet.class).map(GSet::values);
        }
    }

    public boolean add(long id, JsonStreamable value) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return false;
        }
        Option<GSet> lwwRegisterOpt = bucket.registry().crdtOpt(id, GSet.class);
        if (lwwRegisterOpt.isDefined()) {
            lwwRegisterOpt.get().add(value);
        }
        return true;
    }
}
