package org.mitallast.queue.rest.netty

import com.google.inject.Inject
import com.typesafe.config.Config
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import org.mitallast.queue.common.logging.LoggingService
import org.mitallast.queue.common.netty.NettyProvider
import org.mitallast.queue.common.netty.NettyServer

class HttpServer @Inject constructor(
    config: Config,
    logging: LoggingService,
    provider: NettyProvider,
    private val serverHandler: HttpServerHandler,
    private val webSocketFrameHandler: WebSocketFrameHandler
) : NettyServer(config, logging, provider, config.getString("rest.host"), config.getInt("rest.port")) {

    override fun channelInitializer(): ChannelInitializer<SocketChannel> {
        return HttpServerInitializer(serverHandler, webSocketFrameHandler)
    }
}
