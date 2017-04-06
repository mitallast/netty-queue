package org.mitallast.queue.crdt.rest;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.queue.common.json.JsonStreamable;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.CrdtService;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.rest.RestController;

import java.util.Optional;

public class RestLWWRegister {
    private final CrdtService crdtService;

    @Inject
    public RestLWWRegister(RestController controller, CrdtService crdtService) {
        this.crdtService = crdtService;

        controller.handler(this::create)
            .param(controller.param().toLong("id"))
            .response(controller.response().either(
                controller.response().created(),
                controller.response().badRequest()
            ))
            .handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/lww-register");

        controller.handler(this::value)
            .param(controller.param().toLong("id"))
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "_crdt/{id}/lww-register/value");

        controller.handler(this::assign)
            .param(controller.param().toLong("id"))
            .param(controller.param().json(JsonStreamable.class))
            .response(controller.response().either(
                controller.response().ok(),
                controller.response().badRequest()
            ))
            .handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/lww-register/value");
    }

    public boolean create(long id) {
        crdtService.createLWWRegister(id);
        return true;
    }

    public Optional<Streamable> value(long id) {
        return crdtService.crdt(id, LWWRegister.class).value();
    }

    public boolean assign(long id, JsonStreamable value) {
        crdtService.crdt(id, LWWRegister.class).assign(value);
        return true;
    }
}
