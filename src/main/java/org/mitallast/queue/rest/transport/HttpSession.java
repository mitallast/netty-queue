package org.mitallast.queue.rest.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.rest.RestResponse;
import org.mitallast.queue.rest.RestSession;

import java.io.IOException;
import java.io.PrintWriter;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.Values.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpSession implements RestSession {

    private final ChannelHandlerContext ctx;
    private final FullHttpRequest httpRequest;

    public HttpSession(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        this.ctx = ctx;
        this.httpRequest = httpRequest;
    }

    private static boolean isKeepAlive(FullHttpRequest request) {
        String headerValue = request.headers().get(HttpHeaders.Names.CONNECTION);
        return KEEP_ALIVE.equalsIgnoreCase(headerValue);
    }

    @Override
    public void sendResponse(RestResponse response) {
        ByteBuf buffer = response.getBuffer();
        DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, response.getResponseStatus(), buffer, false);
        httpResponse.headers().set(response.getHeaders());
        httpResponse.headers().set(CONTENT_LENGTH, httpResponse.content().readableBytes());
        write(httpResponse);
    }

    @Override
    public void sendResponse(Throwable response) {
        response.printStackTrace(System.err);
        ByteBuf buffer = Unpooled.buffer();
        try (ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer)) {
            try (PrintWriter printWriter = new PrintWriter(outputStream)) {
                response.printStackTrace(printWriter);
            }
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
        DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, buffer, false);
        httpResponse.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
        httpResponse.headers().set(CONTENT_LENGTH, httpResponse.content().readableBytes());
        write(httpResponse);
    }

    private void write(DefaultFullHttpResponse httpResponse) {
        boolean isKeepAlive = isKeepAlive(httpRequest);
        if (isKeepAlive) {
            httpResponse.headers().set(CONTENT_LENGTH, httpResponse.content().readableBytes());
            httpResponse.headers().set(CONNECTION, KEEP_ALIVE);
        } else {
            httpResponse.headers().set(CONNECTION, HttpHeaders.Values.CLOSE);
        }
        ChannelFuture future = ctx.write(httpResponse);

        // Decide whether to close the connection or not.
        if (!isKeepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
