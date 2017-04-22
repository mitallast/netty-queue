package org.mitallast.queue.crdt.rest;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.queue.crdt.CrdtService;
import org.mitallast.queue.rest.RestController;

import javax.inject.Inject;

public class RestCrdtRouting {

    @Inject
    public RestCrdtRouting(RestController controller, CrdtService crdtService) {
        controller.handle(crdtService::routingTable)
            .apply(controller.response().json())
            .handle(HttpMethod.GET, "_crdt/routing");
    }
}
