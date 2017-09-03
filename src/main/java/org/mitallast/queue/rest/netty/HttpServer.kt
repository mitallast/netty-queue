package org.mitallast.queue.rest.netty

import com.google.inject.Inject
import com.typesafe.config.Config
import io.netty.channel.Channel
import io.netty.channel.ChannelInitializer
import org.mitallast.queue.common.netty.NettyProvider
import org.mitallast.queue.common.netty.NettyServer

class HttpServer @Inject constructor(config: Config, provider: NettyProvider, private val serverHandler: HttpServerHandler) :
    NettyServer(config, provider, config.getString("rest.host"), config.getInt("rest.port")) {

    override fun channelInitializer(): ChannelInitializer<Channel> {
        return HttpServerInitializer(serverHandler)
    }
}
