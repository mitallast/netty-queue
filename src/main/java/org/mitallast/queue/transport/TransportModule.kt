package org.mitallast.queue.transport

import com.google.inject.AbstractModule
import org.mitallast.queue.transport.netty.NettyTransportServer
import org.mitallast.queue.transport.netty.NettyTransportService

class TransportModule : AbstractModule() {
    override fun configure() {
        // implementation instance
        bind(NettyTransportServer::class.java).asEagerSingleton()
        bind(TransportController::class.java).asEagerSingleton()
        bind(NettyTransportService::class.java).asEagerSingleton()

        // interface inject
        bind(TransportServer::class.java).to(NettyTransportServer::class.java)
        bind(TransportService::class.java).to(NettyTransportService::class.java)
    }
}
