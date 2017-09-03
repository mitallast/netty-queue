package org.mitallast.queue.crdt.rest;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import javaslang.concurrent.Future;
import javaslang.control.Option;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.json.JsonMessage;
import org.mitallast.queue.crdt.CrdtService;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.routing.ResourceType;
import org.mitallast.queue.rest.RestController;

public class RestLWWRegister {
    private final CrdtService crdtService;

    @Inject
    public RestLWWRegister(
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
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/lww-register");

        controller.handle(
            this::value,
            controller.param().toLong("id"),
            controller.response().optionalJson()
        ).handle(HttpMethod.GET, "_crdt/{id}/lww-register/value");

        controller.handle(
            this::assign,
            controller.param().toLong("id"),
            controller.param().json(JsonMessage.class),
            controller.response().either(
                controller.response().ok(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/lww-register/value");
    }

    public Future<Boolean> create(long id) {
        return crdtService.addResource(id, ResourceType.LWWRegister);
    }

    public Option<Message> value(long id) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return Option.none();
        } else {
            return bucket.registry().crdtOpt(id, LWWRegister.class).flatMap(LWWRegister::value);
        }
    }

    public boolean assign(long id, JsonMessage value) {
        Bucket bucket = crdtService.bucket(id);
        if (bucket == null) {
            return false;
        }
        Option<LWWRegister> lwwRegisterOpt = bucket.registry().crdtOpt(id, LWWRegister.class);
        if (lwwRegisterOpt.isDefined()) {
            lwwRegisterOpt.get().assign(value, System.currentTimeMillis());
        }
        return true;
    }
}
