package org.mitallast.queue.rest.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.rest.RestResponse;
import org.mitallast.queue.rest.RestSession;

import java.io.IOException;
import java.io.PrintWriter;

import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpSession implements RestSession {

    private final ChannelHandlerContext ctx;
    private final FullHttpRequest httpRequest;

    public HttpSession(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        this.ctx = ctx;
        this.httpRequest = httpRequest;
    }

    @Override
    public void sendResponse(RestResponse response) {
        ByteBuf buffer = response.getBuffer();
        DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(
            HTTP_1_1, response.getResponseStatus(), buffer, false, true);

        int bytes = httpResponse.content().readableBytes();
        if (bytes >= 0) {
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes);
        }
        httpResponse.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        if (!httpRequest.protocolVersion().isKeepAliveDefault()) {
            boolean isKeepAlive = KEEP_ALIVE.equalsIgnoreCase(httpRequest.headers().get(HttpHeaderNames.CONNECTION));
            if (isKeepAlive) {
                httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            } else {
                httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
                ctx.write(httpResponse).addListener(ChannelFutureListener.CLOSE);
                return;
            }
        }
        ctx.writeAndFlush(httpResponse, ctx.voidPromise());
    }

    @Override
    public void sendResponse(Throwable response) {
        ByteBuf buffer = Unpooled.buffer();
        try (ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer)) {
            try (PrintWriter printWriter = new PrintWriter(outputStream)) {
                response.printStackTrace(printWriter);
            }
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
        DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(
            HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, buffer, false, true);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content().readableBytes());
        httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        ctx.writeAndFlush(httpResponse).addListener(ChannelFutureListener.CLOSE);
    }
}
