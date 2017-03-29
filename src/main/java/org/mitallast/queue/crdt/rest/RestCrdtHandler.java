package org.mitallast.queue.crdt.rest;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.LongStreamable;
import org.mitallast.queue.crdt.commutative.CmRDTService;
import org.mitallast.queue.rest.RestController;

import java.util.Optional;

public class RestCrdtHandler {
    private final CmRDTService cmRDTService;

    @Inject
    public RestCrdtHandler(RestController controller, CmRDTService cmRDTService) {
        this.cmRDTService = cmRDTService;

        controller.handler(this::assign)
            .param(controller.param().toLong("value"))
            .response((request, response) -> request.response().status(HttpResponseStatus.OK).empty())
            .handle(HttpMethod.POST, "_crdt/{value}");

        controller.handler(this::value)
            .response(controller.response().optionalJson())
            .handle(HttpMethod.GET, "_crdt");
    }

    public boolean assign(long value) {
        cmRDTService.assign(new LongStreamable(value));
        return true;
    }

    public Optional<Streamable> value() {
        return Optional.ofNullable(cmRDTService.value());
    }
}
