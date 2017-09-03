package org.mitallast.queue.rest.netty

import io.netty.channel.Channel
import io.netty.channel.ChannelInitializer
import io.netty.handler.codec.http.HttpObjectAggregator
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.stream.ChunkedWriteHandler

class HttpServerInitializer(private val httpHandler: HttpServerHandler) : ChannelInitializer<Channel>() {
    override fun initChannel(ch: Channel) {
        val pipeline = ch.pipeline()
        pipeline.addLast(HttpServerCodec(4096, 8192, 8192, false))
        pipeline.addLast(HttpObjectAggregator(65536))
        pipeline.addLast(ChunkedWriteHandler())
        pipeline.addLast(httpHandler)
    }
}