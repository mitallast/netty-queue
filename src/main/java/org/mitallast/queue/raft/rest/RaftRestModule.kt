package org.mitallast.queue.raft.rest

import com.google.inject.AbstractModule

class RaftRestModule : AbstractModule() {

    override fun configure() {
        bind(RaftHandler::class.java).asEagerSingleton()
    }
}
