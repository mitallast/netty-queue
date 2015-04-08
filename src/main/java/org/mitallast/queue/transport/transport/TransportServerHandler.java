package org.mitallast.queue.transport.transport;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.mitallast.queue.transport.TransportController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class TransportServerHandler extends SimpleChannelInboundHandler<TransportFrame> {

    private final static Logger logger = LoggerFactory.getLogger(TransportServerHandler.class);
    private final TransportController transportController;

    public TransportServerHandler(TransportController transportController) {
        this.transportController = transportController;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TransportFrame request) throws Exception {
        if (request.isPing()) {
            ctx.write(TransportFrame.of(request), ctx.voidPromise());
        }
        TransportChannel channel = new TransportChannel(ctx, request);
        transportController.dispatchRequest(channel, request);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("unexpected channel error, close channel", cause);
        ctx.close();
    }
}
