package org.mitallast.queue.common.json

import com.google.inject.AbstractModule
import org.mitallast.queue.common.codec.Codec

class JsonModule : AbstractModule() {

    override fun configure() {
        bind(JsonService::class.java).asEagerSingleton()
    }

    companion object {
        init {
            Codec.register(6000, JsonMessage::class.java, JsonMessage.codec)
        }
    }
}
