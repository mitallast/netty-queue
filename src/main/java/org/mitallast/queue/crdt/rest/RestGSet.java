package org.mitallast.queue.crdt.rest;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import javaslang.collection.Set;
import javaslang.concurrent.Future;
import javaslang.control.Option;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.json.JsonMessage;
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

        controller.handle(
            this::create,
            controller.param().toLong("id"),
            controller.response().futureEither(
                controller.response().created(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-set");

        controller.handle(
            this::values,
            controller.param().toLong("id"),
            controller.response().optionalJson()
        ).handle(HttpMethod.GET, "_crdt/{id}/g-set/values");

        controller.handle(
            this::add,
            controller.param().toLong("id"),
            controller.param().json(JsonMessage.class),
            controller.response().either(
                controller.response().ok(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-set/add");
    }

    public Future<Boolean> create(long id) {
        return crdtService.addResource(id, ResourceType.GSet);
    }

    public Option<Set<Message>> values(long id) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return Option.none();
        } else {
            return bucket.registry().crdtOpt(id, GSet.class).map(GSet::values);
        }
    }

    public boolean add(long id, JsonMessage value) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return false;
        }
        Option<GSet> set = bucket.registry().crdtOpt(id, GSet.class);
        if (set.isDefined()) {
            set.get().add(value);
        }
        return true;
    }
}
