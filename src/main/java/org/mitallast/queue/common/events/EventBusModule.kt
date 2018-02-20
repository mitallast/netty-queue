package org.mitallast.queue.common.events

import com.google.inject.AbstractModule

class EventBusModule : AbstractModule() {
    override fun configure() {
        bind(DefaultEventBus::class.java).asEagerSingleton()
        bind(EventBus::class.java).to(DefaultEventBus::class.java)
    }
}
