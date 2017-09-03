package org.mitallast.queue.rest

import com.google.inject.AbstractModule
import org.mitallast.queue.rest.action.ResourceHandler
import org.mitallast.queue.rest.action.SettingsAction
import org.mitallast.queue.rest.netty.HttpServer
import org.mitallast.queue.rest.netty.HttpServerHandler

class RestModule : AbstractModule() {
    override fun configure() {
        bind(HttpServer::class.java).asEagerSingleton()
        bind(HttpServerHandler::class.java).asEagerSingleton()
        bind(RestController::class.java).asEagerSingleton()

        bind(ResourceHandler::class.java).asEagerSingleton()
        bind(SettingsAction::class.java).asEagerSingleton()
    }
}
