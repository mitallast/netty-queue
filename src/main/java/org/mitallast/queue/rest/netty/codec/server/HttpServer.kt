package org.mitallast.queue.rest.netty.codec.server

import com.google.inject.Inject
import com.typesafe.config.Config
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import org.mitallast.queue.common.netty.NettyProvider
import org.mitallast.queue.common.netty.NettyServer

class HttpServer @Inject constructor(
    config: Config,
    provider: NettyProvider,
    private val serverHandler: HttpServerHandler
) :
    NettyServer(config, provider, config.getString("rest.custom.host"), config.getInt("rest.custom.port")) {

    override fun channelInitializer(): ChannelInitializer<SocketChannel> {
        return HttpServerInitializer(serverHandler)
    }
}
