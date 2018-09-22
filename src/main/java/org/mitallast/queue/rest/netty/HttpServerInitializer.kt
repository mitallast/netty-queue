package org.mitallast.queue.rest.netty

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.HttpObjectAggregator
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler
import io.netty.handler.stream.ChunkedWriteHandler
import io.netty.util.concurrent.DefaultEventExecutorGroup
import io.netty.util.concurrent.DefaultThreadFactory

class HttpServerInitializer(
    private val httpHandler: HttpServerHandler,
    private val webSocketFrameHandler: WebSocketFrameHandler
) : ChannelInitializer<SocketChannel>() {

    private val group = DefaultEventExecutorGroup(8, DefaultThreadFactory("http handler"));

    override fun initChannel(ch: SocketChannel) {
        val pipeline = ch.pipeline()
        pipeline.addLast(HttpServerCodec(4096, 8192, 8192, false))
        pipeline.addLast(HttpObjectAggregator(65536))
        pipeline.addLast(ChunkedWriteHandler())
        pipeline.addLast(WebSocketServerCompressionHandler())
        pipeline.addLast(WebSocketServerProtocolHandler("/ws/", null, true))
        pipeline.addLast(webSocketFrameHandler)
        pipeline.addLast(group, httpHandler)
    }
}