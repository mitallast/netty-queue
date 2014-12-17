package org.mitallast.queue.stomp.transport;

import com.google.inject.Inject;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.stomp.DefaultStompFrame;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import org.mitallast.queue.stomp.StompController;
import org.mitallast.queue.stomp.StompSubscriptionController;

@ChannelHandler.Sharable
public class StompServerHandler extends SimpleChannelInboundHandler<StompFrame> {

    private final StompController stompController;
    private final StompSubscriptionController subscriptionController;

    @Inject
    public StompServerHandler(StompController stompController, StompSubscriptionController subscriptionController) {
        this.stompController = stompController;
        this.subscriptionController = subscriptionController;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, StompFrame request) throws Exception {
        StompSession session = new StompSession(ctx, request, subscriptionController);
        try {
            if (!request.decoderResult().isSuccess()) {
                if (request.decoderResult().cause() != null) {
                    session.sendError(request.decoderResult().cause());
                } else {
                    session.sendError("Error decode request");
                }
                return;
            }
            stompController.dispatchRequest(session, request);
        } catch (Throwable e) {
            session.sendError(e);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        subscriptionController.unsubscribe(ctx.channel());
        super.channelUnregistered(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        subscriptionController.unsubscribe(ctx.channel());
        cause.printStackTrace();
        if (ctx.channel().isActive()) {
            StompFrame response = new DefaultStompFrame(StompCommand.ERROR);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
        ctx.close();
    }
}
