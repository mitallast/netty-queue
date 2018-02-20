package org.mitallast.queue.crdt.rest

import com.google.inject.Inject
import io.netty.handler.codec.http.HttpMethod
import io.vavr.collection.Set
import io.vavr.concurrent.Future
import io.vavr.control.Option
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.json.JsonMessage
import org.mitallast.queue.crdt.CrdtService
import org.mitallast.queue.crdt.commutative.GSet
import org.mitallast.queue.crdt.routing.ResourceType
import org.mitallast.queue.rest.RestController

class RestGSet @Inject constructor(controller: RestController, private val crdtService: CrdtService) {

    init {
        controller.handle(
            { id: Long -> this.create(id) },
            controller.param().toLong("id"),
            controller.response().futureEither(
                controller.response().created(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-set")

        controller.handle(
            { id: Long -> this.values(id) },
            controller.param().toLong("id"),
            controller.response().optionalJson()
        ).handle(HttpMethod.GET, "_crdt/{id}/g-set/values")

        controller.handle(
            { id: Long, value: JsonMessage -> this.add(id, value) },
            controller.param().toLong("id"),
            controller.param().json(JsonMessage::class.java),
            controller.response().either(
                controller.response().ok(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/g-set/add")
    }

    private fun create(id: Long): Future<Boolean> {
        return crdtService.addResource(id, ResourceType.GSet)
    }

    private fun values(id: Long): Option<Set<Message>> {
        val bucket = crdtService.bucket(id)
        return if (bucket == null) {
            Option.none()
        } else {
            bucket.registry().crdtOpt(id, GSet::class.java).map { it.values() }
        }
    }

    private fun add(id: Long, value: JsonMessage): Boolean {
        val bucket = crdtService.bucket(id) ?: return false
        val set = bucket.registry().crdtOpt(id, GSet::class.java)
        if (set.isDefined) {
            set.get().add(value)
        }
        return true
    }
}
