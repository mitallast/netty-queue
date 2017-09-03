package org.mitallast.queue.crdt.rest

import com.google.inject.AbstractModule

class RestCrdtModule : AbstractModule() {
    override fun configure() {
        bind(RestCrdtRouting::class.java).asEagerSingleton()
        bind(RestLWWRegister::class.java).asEagerSingleton()
        bind(RestGCounter::class.java).asEagerSingleton()
        bind(RestGSet::class.java).asEagerSingleton()
    }
}
