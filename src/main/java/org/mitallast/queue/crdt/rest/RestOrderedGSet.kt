package org.mitallast.queue.crdt.rest

import com.google.inject.Inject
import io.netty.handler.codec.http.HttpMethod
import javaslang.collection.Vector
import javaslang.concurrent.Future
import javaslang.control.Option
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.json.JsonMessage
import org.mitallast.queue.crdt.CrdtService
import org.mitallast.queue.crdt.commutative.OrderedGSet
import org.mitallast.queue.crdt.routing.ResourceType
import org.mitallast.queue.rest.RestController

class RestOrderedGSet @Inject constructor(controller: RestController, private val crdtService: CrdtService) {

    init {
        controller.handle(
            { id: Long -> this.create(id) },
            controller.param().toLong("id"),
            controller.response().futureEither(
                controller.response().created(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/ordered-g-set")

        controller.handle(
            { id: Long -> this.values(id) },
            controller.param().toLong("id"),
            controller.response().optionalJson()
        ).handle(HttpMethod.GET, "_crdt/{id}/ordered-g-set/values")

        controller.handle(
            { id: Long, timestamp: Long, value: JsonMessage -> this.add(id, timestamp, value) },
            controller.param().toLong("id"),
            controller.param().toLong("timestamp"),
            controller.param().json(JsonMessage::class.java),
            controller.response().either(
                controller.response().ok(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/ordered-g-set/add")
    }

    private fun create(id: Long): Future<Boolean> {
        return crdtService.addResource(id, ResourceType.OrderedGSet)
    }

    private fun values(id: Long): Option<Vector<Message>> {
        val bucket = crdtService.bucket(id)
        return if (bucket == null) {
            Option.none()
        } else {
            bucket.registry().crdtOpt(id, OrderedGSet::class.java).map { it.values() }
        }
    }

    private fun add(id: Long, timestamp: Long, value: JsonMessage): Boolean {
        val bucket = crdtService.bucket(id) ?: return false
        val set = bucket.registry().crdtOpt(id, OrderedGSet::class.java)
        if (set.isDefined) {
            set.get().add(value, timestamp)
        }
        return true
    }
}
