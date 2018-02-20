package org.mitallast.queue.rest.action

import com.google.inject.Inject
import com.typesafe.config.Config
import io.netty.handler.codec.http.HttpMethod
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import org.mitallast.queue.rest.RestController

class SettingsAction @Inject constructor(private val config: Config, controller: RestController) {

    init {

        controller.handle(
            { this.settings() },
            controller.response().json()
        ).handle(HttpMethod.GET, "_settings")
    }

    private fun settings(): Map<String, Boolean> {
        return HashMap.of(
            "raft", config.getBoolean("raft.enabled"),
            "crdt", config.getBoolean("crdt.enabled")
        )
    }
}
