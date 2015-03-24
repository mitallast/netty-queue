package org.mitallast.queue.stomp.transport;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.stomp.DefaultStompFrame;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.mitallast.queue.common.Strings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.stomp.StompSubscriptionController;

import java.nio.charset.Charset;

public class StompSession {

    private final static Charset UTF8 = Charset.forName("UTF-8");

    private final ChannelHandlerContext ctx;
    private final StompFrame request;
    private final StompSubscriptionController subscriptionController;
    private ChannelFuture channelFuture;

    public StompSession(ChannelHandlerContext ctx, StompFrame request, StompSubscriptionController subscriptionController) {
        this.ctx = ctx;
        this.request = request;
        this.subscriptionController = subscriptionController;
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
        String receiptId = Strings.toString(request.headers().get(StompHeaders.RECEIPT));
        if (Strings.isEmpty(receiptId)) {
            response.headers().set(StompHeaders.RECEIPT_ID, receiptId);
        }
        if (response.content().isReadable()) {
            response.headers().setInt(StompHeaders.CONTENT_LENGTH, response.content().readableBytes());
        }
        channelFuture = ctx.write(response, ctx.voidPromise());
    }

    public void close() {
        channelFuture = channelFuture.addListener(ChannelFutureListener.CLOSE);
    }

    public void subscribe(Queue queue) {
        subscriptionController.subscribe(queue, ctx.channel());
    }

    public void unsubscribe(Queue queue) {
        subscriptionController.unsubscribe(queue, ctx.channel());
    }

    public void unsubscribe() {
        subscriptionController.unsubscribe(ctx.channel());
    }
}
