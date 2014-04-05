package org.mitallast.transport.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.mitallast.transport.Request;
import org.mitallast.transport.Response;
import org.mitallast.transport.http.route.Action;
import org.mitallast.transport.http.route.RouteResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@ChannelHandler.Sharable
public class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger logger = LoggerFactory.getLogger(HttpServerHandler.class);

    private static final String KeepAliveValue = HttpHeaders.Values.KEEP_ALIVE.toString();

    private final ObjectMapper mapper = new ObjectMapper();

    private final RouteResolver routeResolver;

    public HttpServerHandler(RouteResolver routeResolver) {
        this.routeResolver = routeResolver;
    }

    private static boolean isKeepAlive(FullHttpRequest request) {
        String headerValue = request.headers().get(HttpHeaders.Names.CONNECTION);
        return KeepAliveValue.equalsIgnoreCase(headerValue);
    }

    private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status.toString() + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        System.out.println(status.toString());
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws Exception {
        if (!httpRequest.getDecoderResult().isSuccess()) {
            sendError(ctx, BAD_REQUEST);
            return;
        }

        Request request = new Request(httpRequest, routeResolver, mapper);
        Response response = new Response();
        Action action = routeResolver.resolve(request);


        try{
            Object result = action.invoke(request, response);
            if (result != null) {
                response.setBody(result);
            }
        } catch (RuntimeException ex){
            logger.error("runtime error", ex);
            response.setException(ex);
        }

        FullHttpResponse httpResponse = buildHttpResponse(response);
        boolean isKeepAlive = isKeepAlive(httpRequest);
        if (isKeepAlive) {
            httpResponse.headers().set(CONTENT_LENGTH, httpResponse.content().readableBytes());
            httpResponse.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        } else {
            httpResponse.headers().set(CONNECTION, HttpHeaders.Values.CLOSE);
        }

        httpResponse.content();

        // Close the connection as soon as the error message is sent.
        ChannelFuture future = ctx.write(httpResponse);

        // Decide whether to close the connection or not.
        if (!isKeepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private FullHttpResponse buildHttpResponse(Response response) throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        try(ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer)){
            if(!response.hasException()){
                try {
                    mapper.writeValue(outputStream, response.getBody());
                    response.getHeaders().set(CONTENT_TYPE, "text/json; charset=UTF-8");
                } catch (IOException e) {
                    logger.error("io error", e);
                    response.setResponseStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    response.setException(e);
                }
            }

            if(response.hasException()){
                response.getHeaders().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
                //noinspection ThrowableResultOfMethodCallIgnored
                outputStream.writeChars(response.getException().toString());
            }

            DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, response.getResponseStatus(), buffer);
            httpResponse.headers().set(response.getHeaders());
            httpResponse.headers().set(CONTENT_LENGTH, httpResponse.content().readableBytes());
            return httpResponse;
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        if (ctx.channel().isActive()) {
            sendError(ctx, INTERNAL_SERVER_ERROR);
        }
        ctx.close();
    }
}
