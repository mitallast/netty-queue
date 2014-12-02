package org.mitallast.queue.stomp.transport;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.stomp.DefaultStompFrame;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;

import java.nio.charset.Charset;

public class StompSession {

    private final static Charset UTF8 = Charset.forName("UTF-8");

    private final ChannelHandlerContext ctx;
    private final StompFrame request;
    private ChannelFuture channelFuture;

    public StompSession(ChannelHandlerContext ctx, StompFrame request) {
        this.ctx = ctx;
        this.request = request;
    }

    public void sendError(Throwable error) {
        error.printStackTrace();
        sendError(error.toString());
    }

    public void sendError(String content) {
        sendResponse(StompCommand.ERROR, content);
    }

    public void sendResponse(StompCommand command) {
        StompFrame response = new DefaultStompFrame(command);
        sendResponse(response);
    }

    public void sendResponse(StompCommand command, String content) {
        StompFrame response = new DefaultStompFrame(command);
        response.content().writeBytes(content.getBytes(UTF8));
        sendResponse(response);
    }

    public void sendResponse(StompFrame response) {
        String receiptId = request.headers().get(StompHeaders.RECEIPT);
        if (receiptId != null && !receiptId.isEmpty()) {
            response.headers().set(StompHeaders.RECEIPT_ID, receiptId);
        }
        if (response.content().isReadable()) {
            response.headers().set(StompHeaders.CONTENT_LENGTH, response.content().readableBytes());
        }
        System.out.println("write response: " + response);
        channelFuture = ctx.writeAndFlush(response, ctx.voidPromise());
    }

    public void close() {
        channelFuture = channelFuture.addListener(ChannelFutureListener.CLOSE);
    }
}
