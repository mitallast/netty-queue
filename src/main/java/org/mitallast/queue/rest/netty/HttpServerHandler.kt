package org.mitallast.queue.rest.netty

import com.google.inject.Inject
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.FullHttpRequest
import io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST
import io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR
import io.netty.handler.codec.http.HttpVersion.HTTP_1_1
import io.netty.util.CharsetUtil
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.rest.RestController

@Suppress("OverridingDeprecatedMember")
@ChannelHandler.Sharable
class HttpServerHandler @Inject constructor(private val restController: RestController) :
    SimpleChannelInboundHandler<FullHttpRequest>(false) {
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

    override fun channelRead0(ctx: ChannelHandlerContext, httpRequest: FullHttpRequest) {
        if (!httpRequest.decoderResult().isSuccess) {
            sendError(ctx, BAD_REQUEST)
            return
        }
        restController.dispatchRequest(ctx, httpRequest)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        if (ctx.channel().isActive) {
            sendError(ctx, INTERNAL_SERVER_ERROR)
        }
        ctx.close()
    }

    private fun sendError(ctx: ChannelHandlerContext, status: HttpResponseStatus) {
        val response = DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status.toString() + "\r\n", CharsetUtil.UTF_8))
        response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8")
        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
        println(status.toString())
    }
}
