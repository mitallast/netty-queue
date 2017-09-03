package org.mitallast.queue.crdt.rest

import io.netty.handler.codec.http.HttpMethod
import org.mitallast.queue.crdt.CrdtService
import org.mitallast.queue.rest.RestController

import javax.inject.Inject

class RestCrdtRouting @Inject constructor(controller: RestController, crdtService: CrdtService) {
    init {
        controller.handle(
            crdtService::routingTable,
            controller.response().json()
        ).handle(HttpMethod.GET, "_crdt/routing")
    }
}
