package org.mitallast.queue.rest.netty

import io.netty.buffer.Unpooled
import io.netty.channel.*
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocketFrame
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import org.mitallast.queue.common.logging.LoggingService

@ChannelHandler.Sharable
class WebSocketFrameHandler(logging: LoggingService) : SimpleChannelInboundHandler<WebSocketFrame>() {
    private val logger = logging.logger()

    @Volatile
    private var channels : Map<ChannelId, Channel> = HashMap.empty()

    @Throws(Exception::class)
    override fun channelRegistered(ctx: ChannelHandlerContext) {
        logger.info("channel registered: {}", ctx.channel())
        synchronized(this) {
            channels = channels.put(ctx.channel().id(), ctx.channel())
        }
        super.channelActive(ctx)
    }

    @Throws(Exception::class)
    override fun channelInactive(ctx: ChannelHandlerContext) {
        logger.info("channel inactive: {}", ctx.channel())
        synchronized(this) {
            channels = channels.remove(ctx.channel().id())
        }
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
                val currentId = ctx.channel().id()
                val content = frame.content()
                val len = content.readableBytes()
                val bytes = ByteArray(len)
                content.readBytes(bytes)

                channels.values()
                    .filter { it.id() != currentId }
                    .forEach { it.writeAndFlush(BinaryWebSocketFrame(Unpooled.wrappedBuffer(bytes)), it.voidPromise()) }
            }
            else -> throw UnsupportedOperationException("unsupported frame type: " + frame.javaClass.simpleName)
        }
    }
}