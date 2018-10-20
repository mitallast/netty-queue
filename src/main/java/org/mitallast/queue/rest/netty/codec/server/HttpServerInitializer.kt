package org.mitallast.queue.rest.netty.codec.server

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import org.mitallast.queue.rest.netty.codec.HttpRequestDecoder
import org.mitallast.queue.rest.netty.codec.HttpResponseEncoder

class HttpServerInitializer(private val httpHandler: HttpServerHandler) : ChannelInitializer<SocketChannel>() {

    override fun initChannel(ch: SocketChannel) {
        val pipeline = ch.pipeline()
        pipeline.addLast(HttpRequestDecoder())
        pipeline.addLast(HttpResponseEncoder())
        pipeline.addLast(httpHandler)
    }
}