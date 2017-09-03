package org.mitallast.queue.crdt.rest;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import javaslang.concurrent.Future;
import javaslang.control.Option;
import org.mitallast.queue.crdt.CrdtService;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.commutative.GCounter;
import org.mitallast.queue.crdt.routing.ResourceType;
import org.mitallast.queue.rest.RestController;

public class RestGCounter {
    private final CrdtService crdtService;

    @Inject
    public RestGCounter(
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
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-counter");

        controller.handle(
            this::value,
            controller.param().toLong("id"),
            controller.response().optional(
                controller.response().text()
            )
        ).handle(HttpMethod.GET, "_crdt/{id}/g-counter/value");

        controller.handle(
            this::increment,
            controller.param().toLong("id"),
            controller.response().optional(
                controller.response().text()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-counter/increment");

        controller.handle(
            this::add,
            controller.param().toLong("id"),
            controller.param().toLong("value"),
            controller.response().optional(
                controller.response().text()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-counter/add");
    }

    public Future<Boolean> create(long id) {
        return crdtService.addResource(id, ResourceType.GCounter);
    }

    public Option<Long> value(long id) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return Option.none();
        } else {
            return bucket.registry().crdtOpt(id, GCounter.class).map(GCounter::value);
        }
    }

    public Option<Long> increment(long id) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return Option.none();
        } else {
            return bucket.registry().crdtOpt(id, GCounter.class).map(GCounter::increment);
        }
    }

    public Option<Long> add(long id, long value) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return Option.none();
        } else {
            return bucket.registry().crdtOpt(id, GCounter.class).map(c -> c.add(value));
        }
    }
}
