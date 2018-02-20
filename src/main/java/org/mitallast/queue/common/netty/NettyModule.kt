package org.mitallast.queue.common.netty

import com.google.inject.AbstractModule

class NettyModule : AbstractModule() {
    override fun configure() {
        bind(NettyProvider::class.java).asEagerSingleton()
    }
}
