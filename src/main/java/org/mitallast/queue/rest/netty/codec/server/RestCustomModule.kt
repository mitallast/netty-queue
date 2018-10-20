package org.mitallast.queue.rest.netty.codec.server

import com.google.inject.AbstractModule

class RestCustomModule : AbstractModule() {
    override fun configure() {
        bind(HttpServer::class.java).asEagerSingleton()
        bind(HttpServerHandler::class.java).asEagerSingleton()
    }
}
