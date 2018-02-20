package org.mitallast.queue.crdt.rest

import com.google.inject.Inject
import io.netty.handler.codec.http.HttpMethod
import io.vavr.concurrent.Future
import io.vavr.control.Option
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.json.JsonMessage
import org.mitallast.queue.crdt.CrdtService
import org.mitallast.queue.crdt.commutative.LWWRegister
import org.mitallast.queue.crdt.routing.ResourceType
import org.mitallast.queue.rest.RestController

class RestLWWRegister @Inject constructor(controller: RestController, private val crdtService: CrdtService) {

    init {
        controller.handle(
            { id: Long -> this.create(id) },
            controller.param().toLong("id"),
            controller.response().futureEither(
                controller.response().created(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/lww-register")

        controller.handle(
            { id: Long -> this.value(id) },
            controller.param().toLong("id"),
            controller.response().optionalJson()
        ).handle(HttpMethod.GET, "_crdt/{id}/lww-register/value")

        controller.handle(
            { id: Long, value: JsonMessage -> this.assign(id, value) },
            controller.param().toLong("id"),
            controller.param().json(JsonMessage::class.java),
            controller.response().either(
                controller.response().ok(),
                controller.response().badRequest()
            )
        ).handle(HttpMethod.POST, HttpMethod.PUT, "_crdt/{id}/lww-register/value")
    }

    fun create(id: Long): Future<Boolean> {
        return crdtService.addResource(id, ResourceType.LWWRegister)
    }

    fun value(id: Long): Option<Message> {
        val bucket = crdtService.bucket(id)
        return if (bucket == null) {
            Option.none()
        } else {
            bucket.registry().crdtOpt(id, LWWRegister::class.java).flatMap { it.value() }
        }
    }

    fun assign(id: Long, value: JsonMessage): Boolean {
        val bucket = crdtService.bucket(id) ?: return false
        val lwwRegisterOpt = bucket.registry().crdtOpt(id, LWWRegister::class.java)
        if (lwwRegisterOpt.isDefined) {
            lwwRegisterOpt.get().assign(value, System.currentTimeMillis())
        }
        return true
    }
}
