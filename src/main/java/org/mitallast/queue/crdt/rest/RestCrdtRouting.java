package org.mitallast.queue.crdt.rest;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.queue.crdt.routing.RoutingService;
import org.mitallast.queue.rest.RestController;

import javax.inject.Inject;

public class RestCrdtRouting {

    @Inject
    public RestCrdtRouting(RestController controller, RoutingService routingService) {
        controller.handler(routingService::routingTable)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "_crdt/routing");
    }
}
