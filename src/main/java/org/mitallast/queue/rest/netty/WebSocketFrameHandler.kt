package org.mitallast.queue.rest.netty

import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocketFrame
import org.apache.logging.log4j.LogManager

@ChannelHandler.Sharable
class WebSocketFrameHandler : SimpleChannelInboundHandler<WebSocketFrame>() {
    private val logger = LogManager.getLogger()

    @Throws(Exception::class)
    override fun channelRegistered(ctx: ChannelHandlerContext) {
        logger.info("channel registered: {}", ctx.channel())
        super.channelActive(ctx)
    }

    @Throws(Exception::class)
    override fun channelInactive(ctx: ChannelHandlerContext) {
        logger.info("channel inactive: {}", ctx.channel())
        super.channelInactive(ctx)
    }

    @Throws(Exception::class)
    override fun channelRead0(ctx: ChannelHandlerContext, frame: WebSocketFrame) {
        when (frame) {
            is TextWebSocketFrame -> {
                val json = frame.text()
                logger.info("received {}", ctx.channel(), json)
            }
            is BinaryWebSocketFrame -> {
                val content = frame.content()
                val len = content.readableBytes()
                val bytes = ByteArray(len)
                content.readBytes(bytes)
                ctx.writeAndFlush(BinaryWebSocketFrame(Unpooled.wrappedBuffer(bytes)), ctx.voidPromise())
            }
            else -> throw UnsupportedOperationException("unsupported frame type: " + frame.javaClass.simpleName)
        }
    }
}