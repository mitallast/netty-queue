package org.mitallast.queue.rest.netty.codec.server

import com.google.inject.Inject
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.util.CharsetUtil
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.rest.netty.codec.*
import java.nio.charset.StandardCharsets

@Suppress("OverridingDeprecatedMember")
@ChannelHandler.Sharable
class HttpServerHandler @Inject constructor() : SimpleChannelInboundHandler<HttpRequest>(false) {
    private val logger = LogManager.getLogger(HttpServerHandler::class.java)

    override fun isSharable(): Boolean {
        return true
    }

    override fun channelRegistered(ctx: ChannelHandlerContext) {
        logger.info("channel registered: {}", ctx.channel())
        super.channelActive(ctx)
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        logger.info("channel inactive: {}", ctx.channel())
        super.channelInactive(ctx)
    }

    override fun channelRead0(ctx: ChannelHandlerContext, httpRequest: HttpRequest) {
        val content = ctx.alloc().buffer()
        content.writeCharSequence("hello world", StandardCharsets.UTF_8)

        val response = HttpResponse(
            httpRequest.version(),
            HttpResponseStatus.OK,
            HttpHeaders.newInstance(),
            content
        )
        response.headers().put(HttpHeaderName.CONTENT_LENGTH, AsciiString.of(Integer.toString(content.readableBytes())))
        if (httpRequest.version() == HttpVersion.HTTP_1_0) {
            response.headers().put(HttpHeaderName.CONNECTION, HttpHeaderValue.CLOSE.ascii())
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
        } else {
            ctx.write(response, ctx.voidPromise())
        }
        httpRequest.release()
    }

    override fun channelReadComplete(ctx: ChannelHandlerContext) {
        ctx.flush()
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        logger.error("unexpected exception", cause)
        if (ctx.channel().isActive) {
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR)
        }
        ctx.close()
    }

    private fun sendError(ctx: ChannelHandlerContext, status: HttpResponseStatus) {
        val response = HttpResponse(
            HttpVersion.HTTP_1_0,
            status,
            HttpHeaders.newInstance(),
            Unpooled.copiedBuffer("Failure: " + status.toString() + "\r\n", CharsetUtil.UTF_8))
        response.headers().set(HttpHeaderName.CONTENT_TYPE, AsciiString.of("text/plain; charset=UTF-8"))
        response.headers().set(HttpHeaderName.CONTENT_LENGTH, AsciiString.of(Integer.toString(response.content().readableBytes())))
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
    }
}
