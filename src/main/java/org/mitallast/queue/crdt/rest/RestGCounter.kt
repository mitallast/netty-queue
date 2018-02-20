package org.mitallast.queue.crdt.rest

import com.google.inject.Inject
import io.netty.handler.codec.http.HttpMethod
import io.vavr.concurrent.Future
import io.vavr.control.Option
import org.mitallast.queue.crdt.CrdtService
import org.mitallast.queue.crdt.commutative.GCounter
import org.mitallast.queue.crdt.routing.ResourceType
import org.mitallast.queue.rest.RestController

class RestGCounter @Inject constructor(controller: RestController, private val crdtService: CrdtService) {

    init {
        controller.handle(
            { id: Long -> this.create(id) },
            controller.param().toLong("id"),
            controller.response().futureEither(
                controller.response().created(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-counter")

        controller.handle(
            { id: Long -> this.value(id) },
            controller.param().toLong("id"),
            controller.response().optional(
                controller.response().text()
            )
        ).handle(HttpMethod.GET, "_crdt/{id}/g-counter/value")

        controller.handle(
            { id: Long -> this.increment(id) },
            controller.param().toLong("id"),
            controller.response().optional(
                controller.response().text()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-counter/increment")

        controller.handle(
            { id: Long, value: Long -> this.add(id, value) },
            controller.param().toLong("id"),
            controller.param().toLong("value"),
            controller.response().optional(
                controller.response().text()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-counter/add")
    }

    private fun create(id: Long): Future<Boolean> {
        return crdtService.addResource(id, ResourceType.GCounter)
    }

    private fun value(id: Long): Option<Long> {
        val bucket = crdtService.bucket(id)
        return if (bucket == null) {
            Option.none()
        } else {
            bucket.registry().crdtOpt(id, GCounter::class.java).map { it.value() }
        }
    }

    private fun increment(id: Long): Option<Long> {
        val bucket = crdtService.bucket(id)
        return if (bucket == null) {
            Option.none()
        } else {
            bucket.registry().crdtOpt(id, GCounter::class.java).map { it.increment() }
        }
    }

    private fun add(id: Long, value: Long): Option<Long> {
        val bucket = crdtService.bucket(id)
        return if (bucket == null) {
            Option.none()
        } else {
            bucket.registry().crdtOpt(id, GCounter::class.java).map { c -> c.add(value) }
        }
    }
}
